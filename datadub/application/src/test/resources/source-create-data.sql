
SET search_path TO src;

insert into country(id, name)
values(1, 'United Kingdom');

insert into zip(id, code, city, country_fk)
values(1, 'WC2N', 'London', 1);

insert into address(id, street, houseidentifier, zip_fk)
values(1, 'Villiers Street', '51', 1);
