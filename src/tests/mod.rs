mod schema;

use crate::DuckDbConnection;
use chrono::{NaiveDate, NaiveDateTime};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;

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

// Common test setup functions
fn setup_basic_connection() -> DuckDbConnection {
    DuckDbConnection::establish(":memory:").unwrap()
}

fn setup_users_table(conn: &mut DuckDbConnection) {
    conn.batch_execute(
        "
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            age INTEGER,
            created_at TIMESTAMP
        )
    ",
    )
    .unwrap();
}

fn setup_orders_table(conn: &mut DuckDbConnection) {
    conn.batch_execute(
        "
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_name VARCHAR,
            quantity INTEGER,
            price DOUBLE,
            order_date DATE
        )
    ",
    )
    .unwrap();
}

fn insert_basic_users(conn: &mut DuckDbConnection) {
    conn.batch_execute(
        "
        INSERT INTO users (id, name, email, age, created_at) VALUES 
        (1, 'John Doe', 'john@example.com', 30, '2025-07-07 20:07:30'),
        (2, 'Jane Smith', 'jane@example.com', 25, '2025-07-07 20:07:30'),
        (3, 'Bob Johnson', 'bob@example.com', 35, '2025-07-07 20:07:30')
    ",
    )
    .unwrap();
}

fn insert_extended_users(conn: &mut DuckDbConnection) {
    conn.batch_execute(
        "
        INSERT INTO users (id, name, email, age, created_at) VALUES 
        (1, 'John Doe', 'john@example.com', 30, '2025-07-07 20:07:30'),
        (2, 'Jane Smith', 'jane@example.com', 25, '2025-07-07 20:07:30'),
        (3, 'Bob Johnson', 'bob@example.com', 35, '2025-07-07 20:07:30'),
        (4, 'Alice Brown', 'alice@example.com', 30, '2025-07-07 20:07:30')
    ",
    )
    .unwrap();
}

fn insert_numbered_users(conn: &mut DuckDbConnection) {
    conn.batch_execute(
        "
        INSERT INTO users (id, name, email, age, created_at) VALUES 
        (1, 'User 1', 'user1@example.com', 21, '2025-07-07 20:07:30'),
        (2, 'User 2', 'user2@example.com', 22, '2025-07-07 20:07:30'),
        (3, 'User 3', 'user3@example.com', 23, '2025-07-07 20:07:30'),
        (4, 'User 4', 'user4@example.com', 24, '2025-07-07 20:07:30'),
        (5, 'User 5', 'user5@example.com', 25, '2025-07-07 20:07:30')
    ",
    )
    .unwrap();
}

fn setup_users_with_basic_data() -> DuckDbConnection {
    let mut conn = setup_basic_connection();
    setup_users_table(&mut conn);
    insert_basic_users(&mut conn);
    conn
}

fn setup_users_with_extended_data() -> DuckDbConnection {
    let mut conn = setup_basic_connection();
    setup_users_table(&mut conn);
    insert_extended_users(&mut conn);
    conn
}

fn setup_users_with_numbered_data() -> DuckDbConnection {
    let mut conn = setup_basic_connection();
    setup_users_table(&mut conn);
    insert_numbered_users(&mut conn);
    conn
}

fn setup_orders_with_sample_data() -> DuckDbConnection {
    let mut conn = setup_basic_connection();
    setup_users_table(&mut conn);
    setup_orders_table(&mut conn);

    // Insert sample users
    conn.batch_execute(
        "
        INSERT INTO users (id, name, email, age, created_at) VALUES 
        (1, 'John Doe', 'john@example.com', 30, '2025-07-07 20:07:30'),
        (2, 'Jane Smith', 'jane@example.com', 25, '2025-07-07 20:07:30')
    ",
    )
    .unwrap();

    // Insert sample orders
    conn.batch_execute(
        "
        INSERT INTO orders (order_id, user_id, product_name, quantity, price, order_date) VALUES 
        (1, 1, 'Laptop', 1, 999.99, '2025-07-07'),
        (2, 1, 'Mouse', 2, 25.50, '2025-07-07'),
        (3, 2, 'Keyboard', 1, 75.00, '2025-07-07')
    ",
    )
    .unwrap();

    conn
}

#[test]
fn test_query() {
    let mut conn = setup_users_with_basic_data();

    let users_under_35 = schema::users::table
        .filter(schema::users::age.lt(35))
        .load::<User>(&mut conn)
        .expect("Error loading users under 35");

    println!("Found {} users under 35", users_under_35.len());
    assert!(users_under_35.len() >= 2); // Should find John and Jane
}

#[test]
fn test_basic_select_all() {
    let mut conn = setup_users_with_basic_data();

    // Test basic select all
    let all_users = schema::users::table
        .load::<User>(&mut conn)
        .expect("Error loading all users");

    assert_eq!(all_users.len(), 3);
}

#[test]
fn test_numeric_filter_operations() {
    let mut conn = setup_users_with_extended_data();

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

#[test]
fn test_limit_and_offset() {
    let mut conn = setup_users_with_numbered_data();

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

#[test]
fn test_orders_table_basic() {
    let mut conn = setup_orders_with_sample_data();

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

#[test]
fn test_empty_result_set() {
    let mut conn = setup_empty_users_table();

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

#[test]
fn test_order_by_clauses() {
    let mut conn = setup_users_for_order_by();

    // Test ORDER BY ASC
    let users_by_age_asc = schema::users::table
        .order(schema::users::age.asc())
        .load::<User>(&mut conn)
        .expect("Error loading users ordered by age ASC");
    assert_eq!(users_by_age_asc.len(), 3);
    // Should be Alice (25), Bob (30), Charlie (35)

    // Test ORDER BY DESC
    let users_by_age_desc = schema::users::table
        .order(schema::users::age.desc())
        .load::<User>(&mut conn)
        .expect("Error loading users ordered by age DESC");
    assert_eq!(users_by_age_desc.len(), 3);
    // Should be Charlie (35), Bob (30), Alice (25)

    // Test ORDER BY with LIMIT
    let oldest_user = schema::users::table
        .order(schema::users::age.desc())
        .limit(1)
        .load::<User>(&mut conn)
        .expect("Error loading oldest user");
    assert_eq!(oldest_user.len(), 1);
    // Should be Charlie
}

#[test]
fn test_string_literal_filtering() {
    let mut conn = setup_users_with_basic_data();

    // Test string literal filtering
    let john_users = schema::users::table
        .filter(schema::users::name.eq("John Doe"))
        .load::<User>(&mut conn)
        .expect("Error loading users filtered by name");

    assert_eq!(john_users.len(), 1);
}

#[test]
fn test_string_operations() {
    let mut conn = setup_users_for_string_operations();

    // Test exact string match
    let john_users = schema::users::table
        .filter(schema::users::name.eq("John Doe"))
        .load::<User>(&mut conn)
        .expect("Error loading users filtered by name");
    assert_eq!(john_users.len(), 1);

    // Test multiple string values
    let young_users = schema::users::table
        .filter(schema::users::age.lt(40))
        .load::<User>(&mut conn)
        .expect("Error loading young users");
    assert_eq!(young_users.len(), 2);

    // Test empty result with string filter
    let nonexistent = schema::users::table
        .filter(schema::users::name.eq("Nobody"))
        .load::<User>(&mut conn)
        .expect("Error loading nonexistent users");
    assert_eq!(nonexistent.len(), 0);
}

fn setup_empty_users_table() -> DuckDbConnection {
    let mut conn = setup_basic_connection();
    setup_users_table(&mut conn);
    // No data insertion - just empty table
    conn
}

fn setup_users_for_order_by() -> DuckDbConnection {
    let mut conn = setup_basic_connection();
    setup_users_table(&mut conn);

    // Insert users with different ages for ordering tests
    conn.batch_execute(
        "
        INSERT INTO users (id, name, email, age, created_at) VALUES 
        (3, 'Charlie', 'charlie@example.com', 35, '2025-07-07 20:07:30'),
        (1, 'Alice', 'alice@example.com', 25, '2025-07-07 20:07:30'),
        (2, 'Bob', 'bob@example.com', 30, '2025-07-07 20:07:30')
    ",
    )
    .unwrap();

    conn
}

fn setup_users_for_string_operations() -> DuckDbConnection {
    let mut conn = setup_basic_connection();
    setup_users_table(&mut conn);

    // Insert users with different names for string operation tests
    conn.batch_execute(
        "
        INSERT INTO users (id, name, email, age, created_at) VALUES 
        (1, 'John Doe', 'john@example.com', 30, '2025-07-07 20:07:30'),
        (2, 'Jane Smith', 'jane@example.com', 25, '2025-07-07 20:07:30'),
        (3, 'Johnny Cash', 'johnny@example.com', 50, '2025-07-07 20:07:30')
    ",
    )
    .unwrap();

    conn
}

#[test]
fn test_deserialized_values() {
    let mut conn = setup_basic_connection();
    setup_users_table(&mut conn);
    setup_orders_table(&mut conn);

    // Insert test data with specific known values (non-null first)
    conn.batch_execute(
        "
        INSERT INTO users (id, name, email, age, created_at) VALUES 
        (42, 'Alice Cooper', 'alice@rock.com', 75, '1948-02-04 12:30:45')
    ",
    )
    .unwrap();

    conn.batch_execute(
        "
        INSERT INTO orders (order_id, user_id, product_name, quantity, price, order_date) VALUES 
        (1001, 42, 'Guitar', 2, 1299.99, '2025-07-10')
    ",
    )
    .unwrap();

    // Test reading and verifying user values
    let users = schema::users::table
        .order(schema::users::id.asc())
        .load::<User>(&mut conn)
        .expect("Error loading users for deserialization test");

    assert_eq!(users.len(), 1);

    // Verify first user (Alice Cooper)
    let alice = &users[0];
    assert_eq!(alice.id, 42);
    assert_eq!(alice.name, Some("Alice Cooper".to_string()));
    assert_eq!(alice.email, Some("alice@rock.com".to_string()));
    assert_eq!(alice.age, Some(75));
    assert_eq!(
        alice.created_at,
        Some(NaiveDateTime::parse_from_str("1948-02-04 12:30:45", "%Y-%m-%d %H:%M:%S").unwrap())
    );

    // Test reading and verifying order values
    let orders = schema::orders::table
        .order(schema::orders::order_id.asc())
        .load::<Order>(&mut conn)
        .expect("Error loading orders for deserialization test");

    assert_eq!(orders.len(), 1);

    // Verify first order (Guitar)
    let guitar_order = &orders[0];
    assert_eq!(guitar_order.order_id, 1001);
    assert_eq!(guitar_order.user_id, Some(42));
    assert_eq!(guitar_order.product_name, Some("Guitar".to_string()));
    assert_eq!(guitar_order.quantity, Some(2));
    assert_eq!(guitar_order.price, Some(1299.99));
    assert_eq!(
        guitar_order.order_date,
        Some(NaiveDate::parse_from_str("2025-07-10", "%Y-%m-%d").unwrap())
    );

    println!("✅ All deserialized values match expected data!");
    println!(
        "  Alice: id={}, name={:?}, age={:?}",
        alice.id, alice.name, alice.age
    );
    println!(
        "  Guitar Order: id={}, price={:?}, date={:?}",
        guitar_order.order_id, guitar_order.price, guitar_order.order_date
    );

    // Test that we can access and verify all individual field types
    println!("📊 Type verification:");
    println!("  Integer (id): {} (type: i32)", alice.id);
    println!(
        "  Optional String (name): {:?} (type: Option<String>)",
        alice.name
    );
    println!(
        "  Optional Integer (age): {:?} (type: Option<i32>)",
        alice.age
    );
    println!(
        "  Optional DateTime: {:?} (type: Option<NaiveDateTime>)",
        alice.created_at
    );
    println!(
        "  Optional Float (price): {:?} (type: Option<f64>)",
        guitar_order.price
    );
    println!(
        "  Optional Date: {:?} (type: Option<NaiveDate>)",
        guitar_order.order_date
    );
}
