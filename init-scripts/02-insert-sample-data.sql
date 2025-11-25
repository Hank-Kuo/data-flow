-- Insert sample data for CDC demonstration

-- Insert customers
INSERT INTO inventory.customers (first_name, last_name, email) VALUES
('John', 'Doe', 'john.doe@email.com'),
('Jane', 'Smith', 'jane.smith@email.com'),
('Mike', 'Johnson', 'mike.johnson@email.com'),
('Sarah', 'Wilson', 'sarah.wilson@email.com'),
('David', 'Brown', 'david.brown@email.com'),
('Lisa', 'Davis', 'lisa.davis@email.com'),
('Tom', 'Miller', 'tom.miller@email.com'),
('Amy', 'Garcia', 'amy.garcia@email.com'),
('Chris', 'Martinez', 'chris.martinez@email.com'),
('Emma', 'Rodriguez', 'emma.rodriguez@email.com');

-- Insert products
INSERT INTO inventory.products (name, description, price, quantity_in_stock, category) VALUES
('Laptop Pro 15"', 'High-performance laptop with 16GB RAM', 1299.99, 50, 'Electronics'),
('Wireless Mouse', 'Ergonomic wireless mouse with long battery life', 29.99, 200, 'Electronics'),
('Office Chair', 'Comfortable ergonomic office chair', 199.99, 25, 'Furniture'),
('Coffee Maker', 'Automatic drip coffee maker with timer', 89.99, 30, 'Appliances'),
('Notebook Set', 'Set of 3 premium notebooks', 24.99, 100, 'Stationery'),
('Desk Lamp', 'LED desk lamp with adjustable brightness', 39.99, 75, 'Furniture'),
('Smartphone Case', 'Protective case for latest smartphone models', 19.99, 150, 'Electronics'),
('Water Bottle', 'Insulated stainless steel water bottle', 24.99, 80, 'Home'),
('Bluetooth Speaker', 'Portable bluetooth speaker with rich sound', 79.99, 40, 'Electronics'),
('Keyboard Mechanical', 'RGB mechanical keyboard for gaming', 129.99, 35, 'Electronics');

-- Insert addresses
INSERT INTO inventory.addresses (customer_id, street_address, city, state, postal_code, country, address_type) VALUES
(1, '123 Main St', 'New York', 'NY', '10001', 'USA', 'HOME'),
(1, '456 Work Ave', 'New York', 'NY', '10002', 'USA', 'WORK'),
(2, '789 Oak Rd', 'Los Angeles', 'CA', '90210', 'USA', 'HOME'),
(3, '321 Pine St', 'Chicago', 'IL', '60601', 'USA', 'HOME'),
(4, '654 Elm Ave', 'Houston', 'TX', '77001', 'USA', 'HOME'),
(5, '987 Maple Dr', 'Phoenix', 'AZ', '85001', 'USA', 'HOME'),
(6, '147 Cedar Ln', 'Philadelphia', 'PA', '19101', 'USA', 'HOME'),
(7, '258 Birch St', 'San Antonio', 'TX', '78201', 'USA', 'HOME'),
(8, '369 Spruce Ave', 'San Diego', 'CA', '92101', 'USA', 'HOME'),
(9, '741 Willow Rd', 'Dallas', 'TX', '75201', 'USA', 'HOME'),
(10, '852 Ash St', 'San Jose', 'CA', '95101', 'USA', 'HOME');

-- Insert orders
INSERT INTO inventory.orders (customer_id, total_amount, status) VALUES
(1, 1329.98, 'COMPLETED'),
(2, 89.99, 'PENDING'),
(3, 224.98, 'SHIPPED'),
(4, 159.98, 'COMPLETED'),
(5, 24.99, 'PENDING'),
(6, 119.98, 'PROCESSING'),
(7, 19.99, 'COMPLETED'),
(8, 104.98, 'SHIPPED'),
(9, 129.99, 'PENDING'),
(10, 249.98, 'COMPLETED');

-- Insert order items
INSERT INTO inventory.order_items (order_id, product_id, quantity, unit_price) VALUES
-- Order 1: Laptop + Mouse
(1, 1, 1, 1299.99),
(1, 2, 1, 29.99),
-- Order 2: Coffee Maker
(2, 4, 1, 89.99),
-- Order 3: Office Chair + Notebook Set
(3, 3, 1, 199.99),
(3, 5, 1, 24.99),
-- Order 4: Desk Lamp + Bluetooth Speaker
(4, 6, 1, 39.99),
(4, 9, 1, 79.99),
-- Order 5: Notebook Set
(5, 5, 1, 24.99),
-- Order 6: Wireless Mouse + Desk Lamp + Water Bottle
(6, 2, 1, 29.99),
(6, 6, 1, 39.99),
(6, 8, 1, 24.99),
-- Order 7: Smartphone Case
(7, 7, 1, 19.99),
-- Order 8: Water Bottle + Bluetooth Speaker
(8, 8, 1, 24.99),
(8, 9, 1, 79.99),
-- Order 9: Mechanical Keyboard
(9, 10, 1, 129.99),
-- Order 10: Laptop Pro + Desk Lamp
(10, 1, 1, 1299.99),
(10, 6, 1, 39.99);