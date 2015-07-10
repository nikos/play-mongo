package models;

import java.util.Date;

import play.modules.mongo.MongoEntity;
import play.modules.mongo.MongoModel;

@MongoEntity("MongoTestEntity")
public class MongoTestEntity extends MongoModel {
	
	public Boolean testBool;
	public Integer testInt;
	public String testStr;
	public Date testDate;
	
}