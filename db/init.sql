CREATE TABLE real_estate (
    id SERIAL PRIMARY KEY,
    property_type VARCHAR(255),
    price INT,
    location VARCHAR(255),
    city VARCHAR(255),
    baths INT,
    purpose VARCHAR(255),
    bedrooms INT,
    area_in_marla FLOAT
);
COPY real_estate(id,property_type, price, location, city, baths, purpose, bedrooms, area_in_marla)
FROM '/data/house_prices.csv' CSV HEADER DELIMITER ',';