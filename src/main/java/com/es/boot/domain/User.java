package com.es.boot.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class User {
    private String name;
    private Integer age;

    public User(String name){
        this.name = name;
    }
}
