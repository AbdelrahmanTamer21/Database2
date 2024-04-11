package sql.parser;

import Main.DBApp;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import sql.antlr.SQLiteLexer;
import sql.antlr.SQLiteParser;

import java.util.Iterator;

public class SQLParser {
    DBApp dbApp;

    public SQLParser(DBApp dbApp) {
        this.dbApp = dbApp;
    }

    public Iterator parseSQL(StringBuffer sql) {
        CharStream stream = CharStreams.fromString(sql.toString());
        SQLiteLexer lexer = new SQLiteLexer(stream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SQLiteParser parser = new SQLiteParser(tokens);
        ParseTree tree = parser.parse();
        DBListener listener = new DBListener(dbApp);
        ParseTreeWalker.DEFAULT.walk(listener, tree);
        return listener.getResult();
    }
}
