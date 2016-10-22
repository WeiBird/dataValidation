/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cdclab.datavalidation;

import java.util.Optional;

/**
 *
 * @author Weli
 */
public class DiffCol<V> {

    private final String key;
    private final V value;
    private final int rowNum;
    

    public DiffCol (final String key, final V value, final int rowNum) {
        this.key = key;
        this.value = value;
        this.rowNum = rowNum;
    }
    
//    public DiffCol create(final String key){
//        return new DiffCol(key,null,0);
//    }

    public String getKey() {
        return this.key;
    }

    public Optional<V> getValue() {
        return (value == null) ? Optional.empty() : Optional.of(value);
    }

    public int getRowNum() {
        return rowNum;
    }

    @Override
    public String toString() {

        StringBuilder str = new StringBuilder()
                .append("Primary Key :").append(getRowNum())
                .append(" ,Key :").append(getKey())
                .append(" ,Value :");
        if (getValue().isPresent()) {
            str.append(getValue().get());
        } else 
            str.append("NULL");
        
        return str.toString();
    }
}
