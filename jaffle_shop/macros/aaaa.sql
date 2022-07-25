
{# ======================================================== #}
{# 宏1：根据传入的跑数频率获取目标表数据记录日期：日频为当日日期，周频为当周周天日期（周一起始），月频为当月月末日期: get_record_dt_by_datepart(datepart) #}
{%- macro get_record_dt_by_datepart(datepart) -%}
    case
        when '{{ datepart }}' = 'day' then date('{{ argv_date }}')
        when '{{ datepart }}' = 'week' then date(date_trunc('week',date('{{ argv_date }}')) + interval '6 D')
        when '{{ datepart }}' = 'month' then date('{{ mon_end }}')
    end
{%- endmacro -%}
{# ======================================================== #}

{# ======================================================== #}
{# 宏2：根据传入的跑数频率获取目标表数据记录频率：日频为'D'，周频为'W'，月频为'M': get_dt_type_by_datepart(datepart) #}
{%- macro get_dt_type_by_datepart(datepart) -%}
    case
        when '{{ datepart }}' = 'day' then 'D'
        when '{{ datepart }}' = 'week' then 'W'
        when '{{ datepart }}' = 'month' then 'M'
    end
{%- endmacro -%}
{# ======================================================== #}
