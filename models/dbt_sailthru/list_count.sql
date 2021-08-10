
with list_stats as (
    select 
    list,
    dormant_count,
    hardbounce_count,
    lists_remove_count,
    new_count,
    optout_loss_count,
    spam_count,
    spam_loss_count
    from (
        select 
            list,
            dormant_count,
            hardbounce_count,
            lists_remove_count,
            new_count,
            optout_loss_count,
            spam_count,
            spam_loss_count,
            row_number() over (partition by list order by count_time desc) as rn
        from `ga---big-query-api-project.ft_sailthru.list_stat`
    ) x
    where x.rn = 1
),
lists as (
    select
        list_id,
        description,
        primary,
        list as list_name,
        type,
        send_time

    from `ga---big-query-api-project.ft_sailthru.list`
),
users as (
    select
        list_id as user_list_id,
        count(user_id) as list_count
    from `ga---big-query-api-project.ft_sailthru.user_list`

    group by 1
),
user_count as (
    select *
        
    from users
    full join lists on lists.list_id = users.user_list_id
),
final as (

    select  *

    from user_count
    full join list_stats on user_count.list_id = list_stats.list
    
)

select
    list_id,
    list_name,
    primary,
    type,
    list_count,
    dormant_count,
    hardbounce_count,
    lists_remove_count,
    new_count,
    optout_loss_count,
    spam_count,
    spam_loss_count,
    send_time,
    description

from final
