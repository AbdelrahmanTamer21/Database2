package sql.parser;

import Main.DBApp;
import Main.Page;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import sql.antlr.SQLiteLexer;
import sql.antlr.SQLiteParser;

import java.util.Iterator;
import java.util.List;

import Exception.DBAppException;

public class SQLParser {
    DBApp dbApp;

    public SQLParser(DBApp dbApp) {
        this.dbApp = dbApp;
    }

    public Iterator parseSQL(StringBuffer sql) throws DBAppException{
        CharStream stream = CharStreams.fromString(sql.toString());
        SQLiteLexer lexer = new SQLiteLexer(stream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLiteParser parser = new SQLiteParser(tokens);
        SQLiteParser.ParseContext parseContext = parser.parse();
        determineStatementType(parseContext);
        ParseTree tree = (ParseTree) parseContext;
        DBListener listener = new DBListener(dbApp);
        ParseTreeWalker.DEFAULT.walk(listener, tree);
        return listener.getResult();
    }

    public String determineStatementType(SQLiteParser.ParseContext parseContext) throws DBAppException {
        // Get the list of sql_stmt_list contexts
        List<SQLiteParser.Sql_stmt_listContext> sqlStmtListContexts = parseContext.sql_stmt_list();

        // Iterate over each sql_stmt_list context
        for (SQLiteParser.Sql_stmt_listContext sqlStmtListContext : sqlStmtListContexts) {
            SQLiteParser.Sql_stmtContext sql_stmtContext = sqlStmtListContext.sql_stmt().get(0);

            // Check if the sql_stmt_list context contains specific types of SQL statements
            if (sql_stmtContext.select_stmt() != null) {
                return "SELECT";
            } else if (sql_stmtContext.insert_stmt() != null) {
                return "INSERT";
            } else if (sql_stmtContext.update_stmt() != null) {
                return "UPDATE";
            } else if (sql_stmtContext.delete_stmt() != null) {
                return "DELETE";
            } else if (sql_stmtContext.create_table_stmt() != null) {
                return "CREATE TABLE";
            } else if (sql_stmtContext.create_index_stmt() != null) {
                return "CREATE INDEX";
            }else {
                throw new DBAppException("Unsupported SQL statement type");
            }
        }

        // If no recognized statement type is found, return null
        return null;
    }
}
