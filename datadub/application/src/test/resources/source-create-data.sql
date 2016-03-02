
SET search_path TO src;

insert into country(id, name)
values(1, 'United Kingdom');

insert into zip(id, code, city, country_fk)
values(1, 'WC2N', 'London', 1);

insert into zip(id, code, city, country_fk)
values(2, 'AL2 1JQ', 'London Colney', 1);

insert into address(id, street, houseidentifier, zip_fk)
values(1, 'Villiers Street', '51', 1);

insert into address(id, street, houseidentifier, zip_fk)
values(2, 'High Street', '196', 2);

insert into producer(id, name, address_fk)
values(1, 'Tesco', 2);

insert into person(id, name, address_fk)
values(1, 'Brian Smith', 1);

insert into person(id, name, address_fk)
values(2, 'Dennis Green', 1);

insert into product(id, itemname, producer_fk)
values(1, 'chainsaw', 1);

insert into product(id, itemname, producer_fk)
values(2, 'milk', 1);

insert into "order"(id, sequence, order_fk, quantity, product_fk)
values(1, 1, 1, '000001');

insert into order_line(id, sequence, order_fk, quantity, product_fk)
values(1, 1, 1, 1, 1);

insert into order_line(id, sequence, order_fk, quantity, product_fk)
values(2, 1, 1, 3, 2);
