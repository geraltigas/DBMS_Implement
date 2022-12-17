package dbms.geraltigas.bean;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class ApplicationContextUtils implements ApplicationContextAware {
    private static ApplicationContext applicationContext;

    public static <T> T newAutoWiredBean(Class<T> clazz) {
        return (T)applicationContext.getAutowireCapableBeanFactory().autowire(clazz, AutowireCapableBeanFactory.AUTOWIRE_AUTODETECT, true);
    }

    public static <T> T autowire(T bean) {
        applicationContext.getAutowireCapableBeanFactory().autowireBeanProperties(bean, AutowireCapableBeanFactory.AUTOWIRE_AUTODETECT, true);
        return bean;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        ApplicationContextUtils.applicationContext = applicationContext;
    }
}
