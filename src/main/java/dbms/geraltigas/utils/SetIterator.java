package dbms.geraltigas.utils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class SetIterator<E> {
    boolean isEnd = false;

    Set<E> set;

    Iterator<E> iterator;

    SetIterator<E> subSetIterator;

    E current;

    public long dataNum() {
        if (subSetIterator == null) {
            return set.size();
        } else {
            return set.size() * subSetIterator.dataNum();
        }
    }

    public SetIterator(Set<E> set,SetIterator<E> subSetIterator) {
        this.set = set;
        this.subSetIterator = subSetIterator;
        iterator = set.iterator();
    }

    public List<E> next() {
        if (subSetIterator == null) {
            if (iterator.hasNext()) {
                current = iterator.next();
            }
            List<E> list = new LinkedList<>();
            list.add(current);
            return list;
        }else {
            if (subSetIterator.hasNext()) {
                List<E> list = subSetIterator.next();
                if (current == null) {
                    current = iterator.next();
                }
                list.add(current);
                return list;
            }else {
                if (iterator.hasNext()) {
                    current = iterator.next();
                    subSetIterator.reset();
                    List<E> list = subSetIterator.next();
                    list.add(current);
                    return list;
                }else {
                    return null;
                }
            }
        }
    }
    public boolean hasNext() {
        if (subSetIterator == null) {
            return iterator.hasNext();
        } else {
            if (subSetIterator.hasNext()) {
                return true;
            } else {
                return iterator.hasNext();
            }
        }
    }
    public void reset() {
        if (subSetIterator != null) {
            subSetIterator.reset();
        }
        iterator = set.iterator();
        current = null;
    }
}
