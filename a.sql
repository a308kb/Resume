--Шаг 1
WITH first_payments as
    (
    select user_id,
                    MIN (transaction_datetime::DATE) as first_payment_date
    from skyeng_db.payments
    where status_name = 'success' -- Надо уточнять, нужно ли это условие тут
    group by 1
    ),
--Шаг 2
all_dates as
    (
    select DISTINCT class_start_datetime::DATE as dt
    from skyeng_db.classes
    where date_part('year', class_start_datetime) = 2016
    order by dt 
    ),
--Шаг 3
    all_dates_by_user as
    (
    select fp.user_id as user_id, 
ad.dt as dt
    from all_dates ad 
    join first_payments fp on ad.dt >= fp.first_payment_date
    order by fp.user_id, dt
    ),
--Шаг 4
payments_by_dates as
(
    select user_id,
                    transaction_datetime::DATE as payment_date, 
                    SUM(classes) as transaction_balance_change 
    from skyeng_db.payments
    where status_name = 'success'  -- Тоже надо уточнять, нужно ли это условие тут (исходя из формулировки - нужно)
    group by 1,2
    order by user_id, payment_date
    ),
--Шаг 5
payments_by_dates_cumsum as
    (
    select ad.user_id,
                    ad.dt,
                    pd.transaction_balance_change,
                    SUM(coalesce(pd.transaction_balance_change,0)) OVER (partition by ad.user_id order by ad.dt rows between unbounded preceding and current row) as transaction_balance_change_cs
    from all_dates_by_user ad
    left join payments_by_dates pd on pd.payment_date = ad.dt and pd.user_id = ad.user_id
    order by ad.user_id, ad.dt),
-- Шаг 6
    classes_by_dates as
    (
    select 
            user_id,
    class_start_datetime::DATE as class_date,
    count (*) * -1 as classes 
    from skyeng_db.classes
    where class_status in ('success', 'failed_by_student')
    and class_type <> 'trial'
    group by 1,2
    ),
--Шаг 7
classes_by_dates_dates_cumsum as
(
    select
    a.user_id,
    a.dt,
    c.classes,
    SUM(coalesce(c.classes,0)) OVER (partition by a.user_id order by a.dt rows between unbounded preceding and current row) as classes_cs
    from all_dates_by_user a
    left join classes_by_dates c on a.user_id=c.user_id and a.dt=c.class_date),
--Шаг 8
    balances as
    (select
            p.user_id,
            p.dt,
            p.transaction_balance_change,
            p.transaction_balance_change_cs,
            c.classes,
            c.classes_cs,
            c.classes_cs + p.transaction_balance_change_cs as balance
    from payments_by_dates_cumsum p
    join classes_by_dates_dates_cumsum c
        on p.user_id=c.user_id
            and p.dt=c.dt)
--Шаг 9
    select 
            dt,
                sum(transaction_balance_change) as sum_transaction_balance_change,
                sum(transaction_balance_change_cs) as sum_transaction_balance_change_cs,
                sum(classes) as sum_classes,
                sum(classes_cs) as sum_classes_cs,
                sum(balance) as sum_balance 
                from balances
                group by dt
                order by dt
