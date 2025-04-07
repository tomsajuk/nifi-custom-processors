CREATE TABLE person (
    id INT,
    name VARCHAR(255),
    age Nullable(INT),
    favorite_color Nullable(VARCHAR(255)),
    dob Nullable(DATE),
    lasttransactiontime Nullable(DateTime64(9)),
    remarks Nullable(String),
    attachments Nullable(String),
) ENGINE = ReplacingMergeTree
ORDER BY id;
