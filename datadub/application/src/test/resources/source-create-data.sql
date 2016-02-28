
SET search_path TO src;

insert into country(id, name)
values(1, 'Denmark');

insert into zip(id, code, city, country_fk)
values(1, '8181', 'Odense', 1);

insert into address(id, street, houseidentifier, zip_fk)
values(1, 'Bandelyst Vej', '44', 1);
