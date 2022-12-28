package dbms.geraltigas.exec.worker.handler;

import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.exception.DropTypeException;
import dbms.geraltigas.exception.DataTypeException;
import dbms.geraltigas.exception.ExpressionException;
import dbms.geraltigas.exception.HandleException;
import net.sf.jsqlparser.statement.Statement;

public interface Handler {


    int handle(Statement query) throws HandleException, DataTypeException, DropTypeException, ExpressionException;

    String getResault(int hash);

    void setDataAccesser(ExecuteEngine executeEngine);

    void setThreadId(long threadId);

}
