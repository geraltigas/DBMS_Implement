package dbms.geraltigas.utils;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SetIteratorTest {

    @Test
    void next() {
        Set<Integer> set1 = Set.of(1, 2, 3);
        Set<Integer> set2 = Set.of(4, 5, 6);
        Set<Integer> set3 = Set.of(7, 8, 9);
        SetIterator<Integer> setIterator1 = new SetIterator<>(set1, null);
        SetIterator<Integer> setIterator2 = new SetIterator<>(set2, setIterator1);
        SetIterator<Integer> setIterator3 = new SetIterator<>(set3, setIterator2);
        int i = 0;
//        for (Integer integer : set3) {
//            System.out.println(integer);
//        }
//        for (Integer integer : set2) {
//            System.out.println(integer);
//        }
//        for (Integer integer : set1) {
//            System.out.println(integer);
//        }
//        while (setIterator2.hasNext()) {
//            List<Integer> list = setIterator2.next();
//            System.out.println(list);
////            i++;
//        }
        setIterator2.reset();
        while(setIterator3.hasNext()) {
            List<Integer> list = setIterator3.next();
            System.out.println(list);
            i++;
        }
        System.out.println(setIterator3.dataNum());
        System.out.println(i);
        assertEquals(setIterator3.dataNum(), i);
    }

}