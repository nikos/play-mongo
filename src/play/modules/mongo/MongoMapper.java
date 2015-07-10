package play.modules.mongo;

import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.ObjectMapper;


public class MongoMapper {

	private static ObjectMapper mapper;
	
	public static <T> T convertValue(Object fromValue, Class<T> toValueType){
		return mapper().convertValue(fromValue, toValueType);
	}
	
	private static ObjectMapper mapper(){
		if (mapper == null){
			mapper = new ObjectMapper();
			mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			mapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_OBJECTS, true);
		}
		return mapper;
	}
	
}
