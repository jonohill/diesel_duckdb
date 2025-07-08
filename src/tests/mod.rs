use crate::DuckDbConnection;
use diesel::prelude::*;
use diesel::connection::SimpleConnection;
use chrono::{NaiveDate, NaiveDateTime};

pub mod schema;

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = schema::users)]
#[diesel(check_for_backend(crate::DuckDb))]
pub struct User {
    pub id: i32,
    pub name: Option<String>,
    pub email: Option<String>,
    pub age: Option<i32>,
    pub created_at: Option<NaiveDateTime>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = schema::orders)]
#[diesel(check_for_backend(crate::DuckDb))]
pub struct Order {
    pub order_id: i32,
    pub user_id: Option<i32>,
    pub product_name: Option<String>,
    pub quantity: Option<i32>,
    pub price: Option<f64>,
    pub order_date: Option<NaiveDate>,
}

// D select * from users;
// ┌───────┬─────────────┬──────────────────┬───────┬─────────────────────────┐
// │  id   │    name     │      email       │  age  │       created_at        │
// │ int32 │   varchar   │     varchar      │ int32 │        timestamp        │
// ├───────┼─────────────┼──────────────────┼───────┼─────────────────────────┤
// │     1 │ John Doe    │ john@example.com │    30 │ 2025-07-07 20:07:30.772 │
// │     2 │ Jane Smith  │ jane@example.com │    25 │ 2025-07-07 20:07:30.772 │
// │     3 │ Bob Johnson │ bob@example.com  │    35 │ 2025-07-07 20:07:30.772 │
// └───────┴─────────────┴──────────────────┴───────┴─────────────────────────┘
// D select * from orders;
// ┌──────────┬─────────┬──────────────┬──────────┬───────────────┬────────────┐
// │ order_id │ user_id │ product_name │ quantity │     price     │ order_date │
// │  int32   │  int32  │   varchar    │  int32   │ decimal(10,2) │    date    │
// ├──────────┼─────────┼──────────────┼──────────┼───────────────┼────────────┤
// │        1 │       1 │ Laptop       │        1 │        999.99 │ 2025-07-07 │
// │        2 │       1 │ Mouse        │        2 │         25.50 │ 2025-07-07 │
// │        3 │       2 │ Keyboard     │        1 │         75.00 │ 2025-07-07 │
// │        4 │       3 │ Monitor      │        1 │        299.99 │ 2025-07-07 │
// └──────────┴─────────┴──────────────┴──────────┴───────────────┴────────────┘


#[cfg(test)]
#[test]
fn test_query() {

    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    
    // Create the users table
    conn.batch_execute("
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            age INTEGER,
            created_at TIMESTAMP
        )
    ").unwrap();
    
    // Insert some test data
    conn.batch_execute("
        INSERT INTO users (id, name, email, age, created_at) VALUES 
        (1, 'John Doe', 'john@example.com', 30, '2025-07-07 20:07:30'),
        (2, 'Jane Smith', 'jane@example.com', 25, '2025-07-07 20:07:30'),
        (3, 'Bob Johnson', 'bob@example.com', 35, '2025-07-07 20:07:30')
    ").unwrap();

    let users_under_35 = schema::users::table
        .filter(schema::users::age.lt(35))
        .load::<User>(&mut conn)
        .expect("Error loading users under 35");
        
    println!("Found {} users under 35", users_under_35.len());
    assert!(users_under_35.len() >= 2); // Should find John and Jane

}

#[cfg(test)]
#[test]
fn test_basic_select_all() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    
    // Create and populate users table
    conn.batch_execute("
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            age INTEGER,
            created_at TIMESTAMP
        )
    ").unwrap();
    
    conn.batch_execute("
        INSERT INTO users (id, name, email, age, created_at) VALUES 
        (1, 'John Doe', 'john@example.com', 30, '2025-07-07 20:07:30'),
        (2, 'Jane Smith', 'jane@example.com', 25, '2025-07-07 20:07:30'),
        (3, 'Bob Johnson', 'bob@example.com', 35, '2025-07-07 20:07:30')
    ").unwrap();

    // Test basic select all
    let all_users = schema::users::table
        .load::<User>(&mut conn)
        .expect("Error loading all users");
        
    assert_eq!(all_users.len(), 3);
}

#[cfg(test)]
#[test]
fn test_numeric_filter_operations() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    
    conn.batch_execute("
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            age INTEGER,
            created_at TIMESTAMP
        )
    ").unwrap();
    
    conn.batch_execute("
        INSERT INTO users (id, name, email, age, created_at) VALUES 
        (1, 'John Doe', 'john@example.com', 30, '2025-07-07 20:07:30'),
        (2, 'Jane Smith', 'jane@example.com', 25, '2025-07-07 20:07:30'),
        (3, 'Bob Johnson', 'bob@example.com', 35, '2025-07-07 20:07:30'),
        (4, 'Alice Brown', 'alice@example.com', 30, '2025-07-07 20:07:30')
    ").unwrap();

    // Test equality filter with integer literal (30 is an i32)
    let users_age_30 = schema::users::table
        .filter(schema::users::age.eq(30))
        .load::<User>(&mut conn)
        .expect("Error loading users age 30");
    assert_eq!(users_age_30.len(), 2); // John and Alice

    // Test greater than
    let users_over_30 = schema::users::table
        .filter(schema::users::age.gt(30))
        .load::<User>(&mut conn)
        .expect("Error loading users over 30");
    assert_eq!(users_over_30.len(), 1); // Bob

    // Test less than or equal
    let users_30_or_under = schema::users::table
        .filter(schema::users::age.le(30))
        .load::<User>(&mut conn)
        .expect("Error loading users 30 or under");
    assert_eq!(users_30_or_under.len(), 3); // John, Jane, Alice
}

#[cfg(test)]
#[test]
fn test_limit_and_offset() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    
    conn.batch_execute("
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            age INTEGER,
            created_at TIMESTAMP
        )
    ").unwrap();
    
    conn.batch_execute("
        INSERT INTO users (id, name, email, age, created_at) VALUES 
        (1, 'User 1', 'user1@example.com', 21, '2025-07-07 20:07:30'),
        (2, 'User 2', 'user2@example.com', 22, '2025-07-07 20:07:30'),
        (3, 'User 3', 'user3@example.com', 23, '2025-07-07 20:07:30'),
        (4, 'User 4', 'user4@example.com', 24, '2025-07-07 20:07:30'),
        (5, 'User 5', 'user5@example.com', 25, '2025-07-07 20:07:30')
    ").unwrap();

    // Test LIMIT
    let limited_users = schema::users::table
        .limit(3)
        .load::<User>(&mut conn)
        .expect("Error loading limited users");
    assert_eq!(limited_users.len(), 3);

    // Test OFFSET
    let offset_users = schema::users::table
        .offset(2)
        .load::<User>(&mut conn)
        .expect("Error loading offset users");
    assert_eq!(offset_users.len(), 3); // Should get users 3, 4, 5

    // Test LIMIT + OFFSET
    let limit_offset_users = schema::users::table
        .limit(2)
        .offset(1)
        .load::<User>(&mut conn)
        .expect("Error loading limit+offset users");
    assert_eq!(limit_offset_users.len(), 2); // Should get users 2, 3
}

#[cfg(test)]
#[test]  
fn test_orders_table_basic() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    
    // Create both tables to test our schema
    conn.batch_execute("
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            age INTEGER,
            created_at TIMESTAMP
        );
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_name VARCHAR,
            quantity INTEGER,
            price DOUBLE,
            order_date DATE
        )
    ").unwrap();
    
    conn.batch_execute("
        INSERT INTO orders (order_id, user_id, product_name, quantity, price, order_date) VALUES 
        (1, 1, 'Laptop', 1, 999.99, '2025-07-07'),
        (2, 1, 'Mouse', 2, 25.50, '2025-07-07'),
        (3, 2, 'Keyboard', 1, 75.00, '2025-07-07')
    ").unwrap();

    // Test basic orders query
    let all_orders = schema::orders::table
        .load::<Order>(&mut conn)
        .expect("Error loading orders");
    assert_eq!(all_orders.len(), 3);

    // Test filtering orders by quantity
    let single_item_orders = schema::orders::table
        .filter(schema::orders::quantity.eq(1))
        .load::<Order>(&mut conn)
        .expect("Error loading single item orders");
    assert_eq!(single_item_orders.len(), 2); // Laptop and Keyboard
}

#[cfg(test)]
#[test]
fn test_empty_result_set() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    
    conn.batch_execute("
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            age INTEGER,
            created_at TIMESTAMP
        )
    ").unwrap();
    
    // Don't insert any data
    
    // Test that empty queries work correctly
    let no_users = schema::users::table
        .load::<User>(&mut conn)
        .expect("Error loading from empty table");
    assert_eq!(no_users.len(), 0);

    // Test that filters on empty tables work
    let filtered_empty = schema::users::table
        .filter(schema::users::age.gt(25))
        .load::<User>(&mut conn)
        .expect("Error filtering empty table");
    assert_eq!(filtered_empty.len(), 0);
}
