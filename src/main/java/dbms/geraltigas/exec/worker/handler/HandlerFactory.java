package dbms.geraltigas.exec.worker.handler;

import dbms.geraltigas.bean.ApplicationContextUtils;
import dbms.geraltigas.dataccess.ExecList;
import dbms.geraltigas.exec.worker.handler.impl.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HandlerFactory {
    @Autowired
    ExecList execList;

    public enum HandlerType {
        CREATE_TABLE,
        CREATE_INDEX,
        INSERT,
        SELECT,
        UPDATE,
        DELETE,
        DROP,
        SHOW
    }

    public Handler getHandler(HandlerType type) {
        switch (type) {
            case CREATE_TABLE:
                CreateTableHandler createTableHandler = new CreateTableHandler();
                createTableHandler.setDataAccesser(execList);
                ApplicationContextUtils.autowire(createTableHandler);
                return createTableHandler;
            case CREATE_INDEX:
                CreateIndexHandler createIndexHandler = new CreateIndexHandler();
                createIndexHandler.setDataAccesser(execList);
                ApplicationContextUtils.autowire(createIndexHandler);
                return createIndexHandler;
            case INSERT:
                InsertHanlder insertHanlder = new InsertHanlder();
                insertHanlder.setDataAccesser(execList);
                ApplicationContextUtils.autowire(insertHanlder);
                return insertHanlder;
            case SELECT:
                SelectHandler selectHandler = new SelectHandler();
                selectHandler.setDataAccesser(execList);
                ApplicationContextUtils.autowire(selectHandler);
                return selectHandler;
            case UPDATE:
                UpdateHandler updateHandler = new UpdateHandler();
                updateHandler.setDataAccesser(execList);
                ApplicationContextUtils.autowire(updateHandler);
                return updateHandler;
            case DELETE:
                DeleteHandler deleteHandler = new DeleteHandler();
                deleteHandler.setDataAccesser(execList);
                ApplicationContextUtils.autowire(deleteHandler);
                return deleteHandler;
            case DROP:
                DropHandler dropHandler = new DropHandler();
                dropHandler.setDataAccesser(execList);
                ApplicationContextUtils.autowire(dropHandler);
                return dropHandler;
            case SHOW:
                ShowHandler showHandler = new ShowHandler();
                showHandler.setDataAccesser(execList);
                ApplicationContextUtils.autowire(showHandler);
                return showHandler;
            default:
                return null;
        }
    }
}
