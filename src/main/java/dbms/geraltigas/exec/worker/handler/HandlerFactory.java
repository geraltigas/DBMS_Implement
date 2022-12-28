package dbms.geraltigas.exec.worker.handler;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.ExecuteEngine;
import dbms.geraltigas.exec.worker.handler.impl.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HandlerFactory {
    @Autowired
    ExecuteEngine executeEngine;

    public enum HandlerType {
        CREATE_TABLE,
        CREATE_INDEX,
        INSERT,
        SELECT,
        UPDATE,
        DELETE,
        DROP,
        SHOW,
        BEGIN,
        COMMIT,
        ROLLBACK,
    }

    public Handler getHandler(HandlerType type) {
        switch (type) {
            case CREATE_TABLE:
                CreateTableHandler createTableHandler = new CreateTableHandler();
                createTableHandler.setDataAccesser(executeEngine);
                ApplicationContextUtils.autowire(createTableHandler);
                return createTableHandler;
            case CREATE_INDEX:
                CreateIndexHandler createIndexHandler = new CreateIndexHandler();
                createIndexHandler.setDataAccesser(executeEngine);
                ApplicationContextUtils.autowire(createIndexHandler);
                return createIndexHandler;
            case INSERT:
                InsertHanlder insertHanlder = new InsertHanlder();
                insertHanlder.setDataAccesser(executeEngine);
                ApplicationContextUtils.autowire(insertHanlder);
                return insertHanlder;
            case SELECT:
                SelectHandler selectHandler = new SelectHandler();
                selectHandler.setDataAccesser(executeEngine);
                ApplicationContextUtils.autowire(selectHandler);
                return selectHandler;
            case UPDATE:
                UpdateHandler updateHandler = new UpdateHandler();
                updateHandler.setDataAccesser(executeEngine);
                ApplicationContextUtils.autowire(updateHandler);
                return updateHandler;
            case DELETE:
                DeleteHandler deleteHandler = new DeleteHandler();
                deleteHandler.setDataAccesser(executeEngine);
                ApplicationContextUtils.autowire(deleteHandler);
                return deleteHandler;
            case DROP:
                DropHandler dropHandler = new DropHandler();
                dropHandler.setDataAccesser(executeEngine);
                ApplicationContextUtils.autowire(dropHandler);
                return dropHandler;
            case SHOW:
                ShowHandler showHandler = new ShowHandler();
                showHandler.setDataAccesser(executeEngine);
                ApplicationContextUtils.autowire(showHandler);
                return showHandler;
            case BEGIN:
                BeginTxnHandler beginTxnHandler = new BeginTxnHandler();
                beginTxnHandler.setDataAccesser(executeEngine);
                ApplicationContextUtils.autowire(beginTxnHandler);
                return beginTxnHandler;
            case COMMIT:
                CommitTxnHandler commitTxnHandler = new CommitTxnHandler();
                commitTxnHandler.setDataAccesser(executeEngine);
                ApplicationContextUtils.autowire(commitTxnHandler);
                return commitTxnHandler;
            case ROLLBACK:
                RollbackTxnHandler rollbackTxnHandler = new RollbackTxnHandler();
                rollbackTxnHandler.setDataAccesser(executeEngine);
                ApplicationContextUtils.autowire(rollbackTxnHandler);
                return rollbackTxnHandler;
            default:
                return null;
        }
    }
}
