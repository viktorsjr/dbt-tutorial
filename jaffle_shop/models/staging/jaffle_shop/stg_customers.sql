
{#
--数据保留策略：
--日维度保留近200天访问用户跟踪数据
--周维度保留近5年访问用户跟踪数据
--月维度保留全量历史数据
 #}
{# 取跑批日和当前自然日较小值，防止传入未来日期误删数据 #}
DELETE FROM dw.dws_user_visit_track_di
WHERE dt_type = {{ get_dt_type_by_datepart('day') }}
AND dt_before <= least({{ get_record_dt_by_datepart('day') }}, date('{{ now_date }}'))- interval '200 D'
;
DELETE FROM dw.dws_user_visit_track_di
WHERE dt_type = {{ get_dt_type_by_datepart('week') }}
AND dt_before <= least({{ get_record_dt_by_datepart('week') }}, date('{{ now_date }}'))- interval '5 Y'
;

{#

 #}

{%- for i in ['day','week','month'] %}
    {# {{ i }}维度数据重跑 #}
    {# 删除{{ i }}维度当期数据 #}
    DELETE FROM dw.dws_user_visit_track_di
    WHERE dt_type = {{ get_dt_type_by_datepart(i) }}
    AND dt = {{ get_record_dt_by_datepart(i) }}
    ;

    {# 临时表1存储{{ i }}维度当期访问数据，dt_before = dt #}
    TRUNCATE TABLE dw.dws_user_visit_track_di_temp1;
    INSERT INTO dw.dws_user_visit_track_di_temp1
    SELECT  {{ get_dt_type_by_datepart(i) }}    as dt_type          --日期频率
            ,{{ get_record_dt_by_datepart(i) }} as dt_before        --历史全量访问日期
            ,{{ get_record_dt_by_datepart(i) }} as dt               --当期访问日期
            ,site_id                            as site_id          --站点
            ,patpat_id                          as patpat_id        --patpat_id
    FROM    dw.dwd_user_visit_detail_di
    WHERE   date_trunc('{{ i }}', dt) = date_trunc('{{ i }}', {{ get_record_dt_by_datepart(i) }})
    GROUP BY    1,2,3,4,5
    ;
    {# 临时表1数据先插入目标表，兼容后续一次回刷两期或多期数据 #}
    INSERT INTO dw.dws_user_visit_track_di
    SELECT * FROM dw.dws_user_visit_track_di_temp1
    ;
    {# 临时表2存储当期访问的站点用户在历史访问数据，dt_before < dt #}
    TRUNCATE TABLE dw.dws_user_visit_track_di_temp2;
    INSERT INTO dw.dws_user_visit_track_di_temp2
    SELECT  {{ get_dt_type_by_datepart(i) }}    as dt_type          --日期频率
            ,t2.dt                              as dt_before        --历史全量访问日期
            ,t1.dt                              as dt               --当期访问日期
            ,t1.site_id                         as site_id          --站点
            ,t1.patpat_id                       as patpat_id        --patpat_id
    FROM    dw.dws_user_visit_track_di_temp1 t1
    INNER JOIN
        (
        SELECT  dt
                ,site_id
                ,patpat_id
        FROM   dw.dws_user_visit_track_di
        WHERE  dt_type = {{ get_dt_type_by_datepart(i) }}
        AND    dt < {{ get_record_dt_by_datepart(i) }}
        GROUP BY   1,2,3
        ) t2
    ON      t1.site_id = t2.site_id
    AND     t1.patpat_id = t2.patpat_id
    WHERE   t2.dt < t1.dt
    ;
    {# 临时表2数据插入目标表 #}
    INSERT INTO dw.dws_user_visit_track_di
    SELECT * FROM dw.dws_user_visit_track_di_temp2
    ;
{%- endfor %}