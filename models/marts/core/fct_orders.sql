with orders as (

select *

from {{ ref('stg_orders')}}
),

payments as (

    select *

    from {{ ref('stg_payments') }}
),

ord_payments as (
    select
        order_id,
        amount
    
    from payments

    group by 1
),

final as (

    select
        orders.order_id,
        orders.customer_id,
        coalesce(ord_payments.amount, 0) as amount
    
    from orders

    left join payments using (order_id)
)

select * 
from final