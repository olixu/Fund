create table info(
    code text primary key,
    full_name text,
    fund_url text,
    tpye text,
    publish_date text,
    setup_date_and scale text,
    asset_scale text,
    amount_scale text,
    company text,
    company_url text,
    bank text,
    bank_url text,
    manager text,
    manager_url text,
    profit_situation text,
    management_feerate text,
    trustee_feerate text,
    standard_compared text,
    followed_target text
);

create table '000001'(
    code text primary key,
    date text,
    total_day text,
    net_value text,
    accumulative_value text,
    rate_day text,
    buy_status text,
    sell_status text,
    profit text
);