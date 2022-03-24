INSERT INTO public.measurands (
measurands_id, measurand, units, display, description, is_core, max_color_value
) OVERRIDING SYSTEM VALUE
VALUES
  ('11','bc','µg/m³','BC','Black Carbon mass concentration','t','3')
, ('21','co2','ppm','CO₂','Carbon Dioxide concentration','f',NULL)
, ('8','co','ppm','CO','Carbon Monoxide concentration','t','11')
, ('28','ch4','ppm','CH₄','Methane concentration','f',NULL)
, ('7','no2','ppm','NO₂','Nitrogen Dioxide concentration','t','0.65')
, ('35','no','ppm','NO','Nitrogen Monoxide concentration','f',NULL)
, ('27','nox','µg/m³','NOx mass','Nitrogen Oxides mass concentration','f',NULL)
, ('10','o3','ppm','O₃','Ozone concentration','t','0.165')
, ('19','pm1','µg/m³','PM1','Particulate matter less than 1 micrometer in diameter mass concentration','f',NULL)
, ('1','pm10','µg/m³','PM10','Particulate matter less than 10 micrometers in diameter mass concentration','t','275')
, ('2','pm25','µg/m³','PM2.5','Particulate matter less than 2.5 micrometers in diameter mass concentration','t','110')
, ('9','so2','ppm','SO₂','Sulfur Dioxide concentration','t','0.22')
, ('37','ambient_temp','deg_c',NULL,NULL,NULL,NULL)
, ('17','bc','ng/m3',NULL,NULL,NULL,NULL)
, ('102','co','ppb',NULL,NULL,NULL,NULL)
, ('14','co2','umol/mol','CO2',NULL,NULL,NULL)
, ('134','humidity','%',NULL,NULL,NULL,NULL)
, ('24','no','ppb',NULL,NULL,NULL,NULL)
, ('15','no2','ppb',NULL,NULL,NULL,NULL)
, ('23','nox','ppb','NOX',NULL,NULL,NULL)
, ('32','o3','ppb',NULL,NULL,NULL,NULL)
, ('676','ozone','ppb',NULL,NULL,NULL,NULL)
, ('36','pm','µg/m³','PM',NULL,NULL,NULL)
, ('131','pm100','µg/m³','PM100',NULL,NULL,NULL)
, ('97','pm25','μg/m³',NULL,NULL,NULL,NULL)
, ('95','pressure','hpa',NULL,NULL,NULL,NULL)
, ('132','pressure','mb',NULL,NULL,NULL,NULL)
, ('98','relativehumidity','%',NULL,NULL,NULL,NULL)
, ('25','rh','%',NULL,NULL,NULL,NULL)
, ('101','so2','ppb',NULL,NULL,NULL,NULL)
, ('100','temperature','c',NULL,NULL,NULL,NULL)
, ('128','temperature','f',NULL,NULL,NULL,NULL)
, ('22','wind_direction','deg',NULL,NULL,NULL,NULL)
, ('34','wind_speed','m/s',NULL,NULL,NULL,NULL)
, ('19840','nox','ppm','NOx','Nitrogen Oxides concentration','f',NULL)
, ('150','voc','iaq',NULL,NULL,NULL,NULL)
, ('19841','bc','ppm',NULL,NULL,NULL,NULL)
, ('33','ufp','particles/cm³','UFP count','Ultrafine Particles count concentration','f',NULL)
, ('29','pn','particles/cm³',NULL,NULL,NULL,NULL)
, ('126','um010','particles/cm³','PM1 count','PM1 count','f',NULL)
, ('130','um025','particles/cm³','PM2.5 count','PM2.5 count','f',NULL)
, ('135','um100','particles/cm³','PM10 count','PM10 count','f',NULL)
, ('125','um003','particles/cm³',NULL,NULL,NULL,NULL)
, ('129','um050','particles/cm³',NULL,NULL,NULL,NULL)
, ('133','um005','particles/cm³',NULL,NULL,NULL,NULL)
, ('4','co','µg/m³','CO mass','Carbon Monoxide mass concentration','f','12163.042264360405')
, ('5','no2','µg/m³','NO₂ mass','Nitrogen Dioxide mass concentration','f','1180.7619365949006')
, ('6','so2','µg/m³','SO₂ mass','Sulfur Dioxide mass concentration','f','556.0245257363534')
, ('3','o3','µg/m³','O₃ mass','Ozone mass concentration','f','312.7641909643373')
, ('19843','no','µg/m³','NO mass','Nitrogen Monoxide mass concentration','f',NULL);

-- need to fix the sequence now
SELECT setval(pg_get_serial_sequence('measurands', 'measurands_id'), (SELECT max(measurands_id) FROM measurands));


-- now lets add some new ones and

INSERT INTO measurands (measurand, units, display, description, is_core) VALUES
( 'wind_speed'
, 'm/s'
, 'ws'
, 'Average wind speed in meters per second'
, true)
, ( 'pressure'
, 'hpa'
, 'atm'
, 'Atmospheric or barometric pressure'
, true)
, ( 'pm10'
, 'ppm'
, 'ppm'
, 'Particles in parts per million'
, true)
, ( 'pm25'
, 'ppm'
, 'ppm'
, 'Particles in parts per million'
, true)
, ( 'wind_direction'
, 'deg'
, 'wd'
, 'Direction that the wind originates from'
, true)
, ( 'so4'
, 'ppb'
, 'SO4'
, 'Sulfate'
, true)
, ( 'ec'
, 'ppb'
, 'EC'
, 'Elemental Carbon'
, true)
, ( 'oc'
, 'ppb'
, 'OC'
, 'Organic Carbon'
, true)
, ( 'cl'
, 'ppb'
, 'Cl'
, 'Chloride'
, true)
, ( 'k'
, 'ppb'
, 'K'
, 'Potassium'
, true)
, ( 'no3'
, 'ppb'
, 'NO3'
, 'Nitrite'
, true)
, ( 'pb'
, 'ppb'
, 'Pb'
, 'Lead'
, true)
, ( 'as'
, 'ppb'
, 'As'
, 'Arsenic'
, true)
, ( 'ca'
, 'ppb'
, 'Ca'
, 'Calcium'
, true)
, ( 'fe'
, 'ppb'
, 'Fe'
, 'Iron'
, true)
, ( 'ni'
, 'ppb'
, 'Ni'
, 'Nickle'
, true)
, ( 'v'
, 'ppb'
, 'V'
, 'Vanadium'
, true)
ON CONFLICT (measurand, units) DO UPDATE
SET description = EXCLUDED.description
, is_core = EXCLUDED.is_core
, display = EXCLUDED.display
;
