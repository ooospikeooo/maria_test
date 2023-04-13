package net.jw.twin.db;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

@Getter
@Setter
@SuperBuilder(toBuilder=true)
@NoArgsConstructor
@ToString
public class AirQuality {
    private            Long                      id;// ( 1)   id                            	BIGINT             아이디
    private         LocalDateTime dateTime;// ( 2)   date_time                     	DATETIME           측정일시
    private            Long       measurePositionId;// ( 3)   measure_position_id           	BIGINT             측정위치ID
    private         Integer               pm10Value;// ( 4)   pm10_value                    	INTEGER            pm10농도
    private          String                pm10Flag;// ( 5)   pm10_flag                     	VARCHAR(20)        pm10상태정보
    private         Integer               pm25Value;// ( 6)   pm25_value                    	INTEGER            pm25농도
    private          String                pm25Flag;// ( 7)   pm25_flag                     	VARCHAR(20)        pm25상태정보
    private         Integer                 o3Value;// ( 8)   o3_value                      	INTEGER            오존농도
    private          String                  o3Flag;// ( 9)   o3_flag                       	VARCHAR(20)        오존상태정보
}
