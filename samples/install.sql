-- ----------------------------------------------------------------------------------------------------------------------------
-- File Name     : install.sql
-- Author        : tiago felicio
-- Description   :
-- Call Syntax   : @install.sql (base-folder) (password) (metadata-tablespace) (data-tablespace) (temporary-tablespace)
-- Last Modified : 2020/03/13
-- ----------------------------------------------------------------------------------------------------------------------------

create user metadata identified by &2
default tablespace &3
temporary tablespace &5
quota unlimited on &3;

grant connect to metadata;
grant create procedure to metadata;

grant execute on out.bind to metadata;
grant execute on out.process to metadata;
grant execute on out.data_integration to metadata;
grant execute on out.files to metadata;
grant execute on out.internet to metadata;
grant execute on out.utilities to metadata;

create user data identified by &2
default tablespace &4
temporary tablespace &5
quota unlimited on &4;

grant connect to data;
grant resource to data;

create table data.dh_world_cities (
    name varchar2(250),
    country varchar2(250),
    subcountry varchar2(250),
    geonameid number,
    resource_name varchar2(250)
);


  CREATE TABLE "DATA"."DH_WORLD_CITIES_2" 
   (	"SK" NUMBER, 
	"NAME" VARCHAR2(250 BYTE), 
	"COUNTRY" VARCHAR2(250 BYTE), 
	"SUBCOUNTRY" VARCHAR2(250 BYTE), 
	"GEONAMEID" NUMBER, 
	"RESOURCE_NAME" VARCHAR2(250 BYTE), 
	"TIAGO" NUMBER, 
	"INES" NUMBER
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 0 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 COMPRESS BASIC NOLOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "DATA" ;


  CREATE TABLE "DATA"."DH_WORLD_CITIES_3" 
   (	"SK" NUMBER, 
	"NAME" VARCHAR2(250 BYTE), 
	"COUNTRY" VARCHAR2(250 BYTE), 
	"SUBCOUNTRY" VARCHAR2(250 BYTE), 
	"GEONAMEID" NUMBER, 
	"COL1" VARCHAR2(50 BYTE), 
	"RESOURCE_NAME" VARCHAR2(250 BYTE)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 0 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 COMPRESS BASIC NOLOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "DATA" ;


  CREATE TABLE "DATA"."DH_WORLD_CITIES_4" 
   (	"SK" NUMBER, 
	"NAME" VARCHAR2(250 BYTE), 
	"COUNTRY" VARCHAR2(250 BYTE), 
	"SUBCOUNTRY" VARCHAR2(250 BYTE), 
	"GEONAMEID" NUMBER, 
	"COL1" VARCHAR2(50 BYTE), 
	"RESOURCE_NAME" VARCHAR2(250 BYTE)
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
  STORAGE(
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "DATA" 
  PARTITION BY LIST ("COUNTRY") 
 (PARTITION "P_ALL"  VALUES (default) SEGMENT CREATION IMMEDIATE 
  PCTFREE 0 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 COMPRESS BASIC NOLOGGING 
  STORAGE(INITIAL 8388608 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "DATA" ) ;


  CREATE TABLE "DATA"."DH_WORLD_CITIES_5" 
   (	"NAME" VARCHAR2(250 BYTE), 
	"COUNTRY" VARCHAR2(250 BYTE), 
	"SUBCOUNTRY" VARCHAR2(250 BYTE), 
	"GEONAMEID" NUMBER, 
	"RESOURCE_NAME" VARCHAR2(250 BYTE)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 0 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 COMPRESS BASIC NOLOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "DATA" ;


@&1/samples/processes/examples.pks
@&1/samples/processes/examples.pkb
