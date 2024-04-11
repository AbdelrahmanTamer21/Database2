package sql.parser;

import Main.DBApp;
import com.opencsv.CSVReader;
import sql.SQLTerm;
import sql.antlr.SQLiteParser;
import sql.antlr.SQLiteParserBaseListener;

import java.io.File;
import java.io.IOException;
import java.util.*;

import Exception.DBAppException;

public class DBListener extends SQLiteParserBaseListener {
    private final DBApp dbApp;
    private Iterator result;
    public DBListener(DBApp dbApp) {
        this.dbApp = dbApp;
    }

    public Iterator getResult() {
        return result;
    }

    /**
     * Helper Methods
     */
    private String formatString(String value){
        value = value.trim();
        if(value.length() > 0 && (value.charAt(0) == '\'' || value.charAt(0) == '\"'))
            value = value.substring(1);
        if(value.length() > 0 && (value.charAt(value.length()-1) == '\'' || value.charAt(value.length()-1) == '\"'))
            value = value.substring(0, value.length()-1);
        return value;
    }
    private Object parseObject(String value){
        try {
            // Check if the value is an integer
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            try {
                // Check if the value is a double
                return Double.parseDouble(value);
            } catch (NumberFormatException e2) {
                // Return the value as a string
                return formatString(value);
            }
        }
    }
    private String parseType(SQLiteParser.Type_nameContext type_name){
        String type = type_name.getText().trim().toLowerCase();
        if (type.equals("int")) {
            return "java.lang.Integer";
        } else if (type.equals("double") || type.equals("float") || type.equals("decimal")) {
            return "java.lang.Double";
        } else if (type.contains("char") || type.contains("text") || type.contains("string") || type.contains("varchar") || type.contains("varying")) {
            return "java.lang.String";
        }
        return null;
    }
    private Object parseTypeFromTable(String tableName, String columnName, String value){
        value = formatString(value);
        ArrayList<String[]> row = new ArrayList<>();
        File file = new File("metadata.csv");
        try(Scanner scanner = new Scanner(file)){
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                String[] values = line.split(",");
                if(values[0].equals(tableName) && values[1].equals(columnName)){
                    switch (values[2]) {
                        case "java.lang.Integer" -> {
                            return Integer.parseInt(value);
                        }
                        case "java.lang.Double" -> {
                            return Double.parseDouble(value);
                        }
                        case "java.lang.String" -> {
                            return value;
                        }
                    }
                }
            }
        }catch (IOException e){
            throw new RuntimeException("Error reading metadata file");
        }
        return null;
    }
    private void parseWhere(ArrayList<SQLiteParser.ExprContext> exprs, ArrayList<String> operators, SQLiteParser.ExprContext where) {
        parseOperation(operators, where);
        parseCondition(exprs, where);
    }
    private void parseCondition(ArrayList<SQLiteParser.ExprContext> conditions, SQLiteParser.ExprContext expr) {
        if(!hasChild(expr)){
            conditions.add(expr);
            return;
        }

        for(int i = 0; i < expr.expr().size(); i++){
            parseCondition(conditions, expr.expr(i));
        }
    }

    private void parseOperation(ArrayList<String> operators, SQLiteParser.ExprContext expr) {
        if(!hasChild(expr)){
            String tmp = CheckANDorORorXOR(expr);
            if(tmp != null)
                operators.add(tmp);
            return;
        }

        for(int i = 0; i < expr.expr().size() && i < 2; i++){
            parseOperation(operators, expr.expr(i));

            if (i == 0) {
                String tmp = CheckANDorORorXOR(expr);
                if(tmp != null)
                    operators.add(tmp);
            }
        }
    }

    private String CheckANDorORorXOR(SQLiteParser.ExprContext expr) {
        if (expr.AND_() != null) {
            return "AND";
        } else if (expr.OR_() != null) {
            return "OR";
        } else if (expr.XOR_() != null) {
            return "XOR";
        }
        return null;
    }

    private boolean hasChild(SQLiteParser.ExprContext expr) {
        return expr.OR_() != null || expr.AND_() != null || expr.XOR_() != null;
    }

    private String chooseSqlOperator(SQLiteParser.ExprContext operator) throws DBAppException {
        if (operator.EQ() != null || operator.ASSIGN() != null) {
            return "=";
        } else if (operator.NOT_EQ1() != null || operator.NOT_EQ2() != null){
            return "!=";
        } else if (operator.LT() != null) {
            return "<";
        } else if (operator.LT_EQ() != null) {
            return "<=";
        } else if (operator.GT() != null) {
            return ">";
        } else if (operator.GT_EQ() != null) {
            return ">=";
        }
        throw new DBAppException("Invalid operator");
    }

    /**
     * Methods for SQL commands
     */
    @Override
    public void enterCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        String clusteringKey = null;

        Hashtable<String, String> columns = new Hashtable<>();
        for (SQLiteParser.Column_defContext column : ctx.column_def()) {
            String columnName = column.column_name().getText();
            String columnType = parseType(column.type_name());
            columns.put(columnName, columnType);
            if (column.column_constraint().size() > 0 && column.column_constraint(0).getText().equalsIgnoreCase("PRIMARYKEY")) {
                clusteringKey = columnName;
            }
        }

        try {
            dbApp.createTable(tableName, clusteringKey, columns);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        result = null;
    }

    @Override
    public void enterCreate_index_stmt(SQLiteParser.Create_index_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        String indexName = ctx.index_name().getText();
        int size = ctx.indexed_column().size();

        if(size == 1){
            String columnName = ctx.indexed_column(0).column_name().getText();
            try {
                dbApp.createIndex(tableName, columnName,indexName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }else{
            throw new RuntimeException("Composite Indexes are not supported");
        }

    }

    @Override
    public void enterInsert_stmt(SQLiteParser.Insert_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        for (int i = 0; i < ctx.values_clause().value_row().size(); i++) {
            if(ctx.column_name().size() != ctx.values_clause().value_row(i).expr().size())
                throw new RuntimeException("Number of columns doesn't match number of values");
            Hashtable<String, Object> values = new Hashtable<>();
            for (int j = 0; j < ctx.values_clause().value_row(i).expr().size(); j++) {
                String columnName = ctx.column_name(j).getText();
                Object value = parseObject(ctx.values_clause().value_row(i).expr(j).getText());
                values.put(columnName, value);
            }
            try {
                dbApp.insertIntoTable(tableName, values);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        result = null;
    }

    @Override
    public void enterUpdate_stmt(SQLiteParser.Update_stmtContext ctx) {
        String tableName = ctx.qualified_table_name().getText();

        String primaryKeyValue;
        int primaryKeyIndex = ctx.column_name().size();
        primaryKeyValue = formatString(ctx.expr(primaryKeyIndex).expr(1).getText());


        Hashtable<String, Object> values = new Hashtable<>();
        for (int i = 0; i < primaryKeyIndex; i++) {
            String columnName = ctx.column_name(i).getText();
            Object value = parseTypeFromTable(tableName,columnName,ctx.expr(i).getText());
            values.put(columnName, value);
        }

        try {
            dbApp.updateTable(tableName, primaryKeyValue, values);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        result = null;
    }

    @Override
    public void enterDelete_stmt(SQLiteParser.Delete_stmtContext ctx) {
        String tableName = ctx.qualified_table_name().getText();
        ArrayList<SQLiteParser.ExprContext> exprs = new ArrayList<>();
        parseCondition(exprs, ctx.expr());

        Hashtable<String, Object> values = new Hashtable<>();
        for (SQLiteParser.ExprContext expr : exprs) {
            String columnName = expr.expr(0).getText();
            Object value = parseTypeFromTable(tableName,columnName,expr.expr(1).getText());
            values.put(columnName, value);
        }

        try {
            dbApp.deleteFromTable(tableName, values);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        result = null;
    }

    @Override
    public void enterSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
        //ArrayList<String> columns = new ArrayList<>();
        ArrayList<SQLiteParser.ExprContext> exprs = new ArrayList<>();
        ArrayList<String> operators = new ArrayList<>();
        String tableName = ctx.select_core(0).table_or_subquery(0).table_name().getText();

//        for (SQLiteParser.Result_columnContext column : ctx.select_core(0).result_column()) {
//            columns.add(column.getText());
//        }

        SQLiteParser.ExprContext where = ctx.select_core(0).expr().get(0);
        parseWhere(exprs, operators, where);

        try {
            SQLTerm[] terms = new SQLTerm[exprs.size()];
            for (int i = 0; i < exprs.size(); i++) {
                SQLiteParser.ExprContext expr = exprs.get(i);
                String columnName = expr.expr(0).getText();
                String operator = chooseSqlOperator(exprs.get(i));
                Object value = parseTypeFromTable(tableName,columnName,expr.expr(1).getText());
                terms[i] = new SQLTerm(tableName, columnName, operator, value);
            }
            result = dbApp.selectFromTable(terms, operators.toArray(new String[0]));
        }catch (DBAppException e){
            throw new RuntimeException(e);
        }
    }
}
