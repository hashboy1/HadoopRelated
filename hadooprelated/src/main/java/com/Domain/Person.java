package com.Domain;

import java.io.Serializable;

public class Person implements Serializable {
   private String Name;
   private int Age;
   
	public Person(String name, int age) {
		
		Name = name;
		Age = age;
	}
	public String getName() {
		return Name;
	}
	public void setName(String name) {
		Name = name;
	}
	public int getAge() {
		return Age;
	}
	public void setAge(int age) {
		Age = age;
	}
	@Override
	public String toString() {
		return "Person [Name=" + Name + ", Age=" + Age + "]";
	}
   
}
