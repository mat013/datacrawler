
drop table order_line
drop table product; 
drop table "order"
drop table person
drop table producer
drop table address
drop table zip
drop table country

SET search_path TO source;


CREATE TABLE country
(
  id numeric NOT NULL,
  name character varying(200),
  CONSTRAINT country_pk PRIMARY KEY (id)
)

CREATE TABLE zip
(
  id numeric NOT NULL,
  code character varying(200),
  city character varying(200),
  country_fk numeric,
  CONSTRAINT zip_pk PRIMARY KEY (id),
  CONSTRAINT zip_country_fk FOREIGN KEY (country_fk) REFERENCES country (id)
)

CREATE TABLE address
(
  id numeric NOT NULL,
  street character varying(200),
  houseidentifier character varying(50),
  zip_fk numeric,
  CONSTRAINT address_pk PRIMARY KEY (id)
)

CREATE TABLE producer
(
  id numeric NOT NULL,
  name character varying(200),
  address_fk numeric,
  CONSTRAINT producer_fk PRIMARY KEY (id),
  CONSTRAINT producet_address_fk FOREIGN KEY (address_fk) REFERENCES address (id)
)

CREATE TABLE person
(
  id numeric NOT NULL,
  name character varying(100),
  address_fk numeric,
  CONSTRAINT person_pk PRIMARY KEY (id),
  CONSTRAINT person_address_fk FOREIGN KEY (address_fk) REFERENCES address (id)
)

CREATE TABLE "order"
(
  id numeric NOT NULL,
  orderid character varying(50),
  responsible_fk_id numeric,
  delivery_fk_id numeric,
  CONSTRAINT order_pk PRIMARY KEY (id),
  CONSTRAINT order_delivery_fk FOREIGN KEY (delivery_fk_id) REFERENCES person (id),
  CONSTRAINT order_responsible_fk FOREIGN KEY (responsible_fk_id) REFERENCES person (id)
)


CREATE TABLE order_line
(
  id numeric NOT NULL,
  sequence numeric NOT NULL,
  order_fk numeric,
  quantity numeric,
  product_fk numeric,
  comment character varying(2000),
  CONSTRAINT order_line_pk PRIMARY KEY (id),
  CONSTRAINT order_line_order_fk FOREIGN KEY (order_fk) REFERENCES "order"
)

CREATE TABLE product
(
  id numeric NOT NULL,
  itemname character varying(200),
  producer_fk numeric,
  CONSTRAINT product_pk PRIMARY KEY (id),
  CONSTRAINT product_producer_fk FOREIGN KEY (producer_fk) REFERENCES producer (id)
)