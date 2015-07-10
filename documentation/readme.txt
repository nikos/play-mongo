The Jackson mapper library has been ugraded but nevertheless it required a patch 
to handle Date serialisation/deserialisation in accordance to Java Mongo Driver date management 
especially regarding query syntax.

A new feature WRITE_DATES_AS_OBJECTS has been added in Jackson Library and used in the play-mongo module 
to be able to use date correctly. Required modification made to Jackson Mpapper libray are 
described in the separate patch file.

NB : For building purpose Joda dependency and classes have also been removed from Jackson Mapper library.     