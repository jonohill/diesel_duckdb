diesel::table! {
    orders (order_id) {
        order_id -> Integer,
        user_id -> Nullable<Integer>,
        product_name -> Nullable<VarChar>,
        quantity -> Nullable<Integer>,
        price -> Nullable<Double>,
        order_date -> Nullable<Date>,
    }
}

diesel::table! {
    users (id) {
        id -> Integer,
        name -> Nullable<VarChar>,
        email -> Nullable<VarChar>,
        age -> Nullable<Integer>,
        created_at -> Nullable<Timestamp>,
    }
}

diesel::joinable!(orders -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(orders, users,);
