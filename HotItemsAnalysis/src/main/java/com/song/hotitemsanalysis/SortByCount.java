package com.song.hotitemsanalysis;

import java.util.Comparator;

public class SortByCount implements Comparator<ItemViewCount> {

    @Override
    public int compare(ItemViewCount o1, ItemViewCount o2) {
        if(o1.getCount() == o2.getCount()) {
            return 0;
        }else {
            if(o1.getCount() < o2.getCount()) {
                return 1;
            }else {
                return -1;
            }
        }
    }
}
