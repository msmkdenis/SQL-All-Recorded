--Задание 1
create or replace procedure update_employees_rate(rate_changes json)
language plpgsql
as $$
    declare
    _array_length integer := json_array_length(rate_changes);
    _base_min_rate integer := 500;

    _employee_rate integer;
    _rate_changer integer;
    _new_rate integer;
    begin
        if _array_length <= 0 then raise exception 'request must not be empty';
        end if;

        for _i in 0.._array_length-1
        loop
            select into _employee_rate (select rate from employees where id = (select rate_changes->_i->>'employee_id')::uuid);
            select into _rate_changer (select rate_changes->_i->>'rate_change');
            select into _new_rate (select _employee_rate + (_employee_rate * _rate_changer / 100));

            if _new_rate < _base_min_rate then
                update employees set rate = _base_min_rate where id = (select rate_changes->_i->>'employee_id')::uuid;
            else
                update employees
                set rate = _employee_rate + (_employee_rate * _rate_changer / 100)
                where id = (select rate_changes->_i->>'employee_id')::uuid;
            end if;
            end loop;
        end;
    $$;

--Задание 2
create or replace procedure indexing_salary(index integer)
    language plpgsql
as $$
declare
    _index_increment integer := 2;
    _current_average_rate integer;
    _row record;
begin
    if index <= 0 then raise exception 'index must be positive';
    end if;

    select into _current_average_rate (select avg(rate) from employees);

    for _row in (select * from employees) loop
    if _row.rate < _current_average_rate then
        update employees set rate = (_row.rate + (_row.rate * (index + _index_increment) / 100)) where id = _row.id;
        else
        update employees set rate = (_row.rate + (_row.rate * index / 100)) where id = _row.id;
    end if;
    end loop;
end
$$;

--Задание 3
create or replace procedure close_project(_id uuid)
    language plpgsql
as $$
declare
    _max_share_bonus integer := 0.75;
    _max_bonus_hours integer := 16;
    _project_actual_time integer;
    _project_estimate_time integer;
    _participants integer;
    _possible_bonus_hours integer;
    _actual_bonus_hours integer;
    _row record;
    begin
    if (select exists(select 1 from projects where id = _id)) = false then raise exception 'project not found';
    end if;

    if (select is_active from projects where id = _id) = false then raise exception 'project is already closed';
    end if;

    select into _project_actual_time (select sum(work_hours) from logs where project_id = _id);
    select into _participants (select count(distinct employee_id) from logs where project_id = _id);
    select into _project_estimate_time (select estimated_time from projects where id = _id);

    if
        _project_estimate_time > _project_actual_time
        and
        _project_estimate_time is not null
    then

        select into _possible_bonus_hours (select div((_project_estimate_time - _project_actual_time), _participants) * _max_share_bonus);

        select into _actual_bonus_hours
            CASE
                when _possible_bonus_hours > _max_bonus_hours then _max_bonus_hours
                else _possible_bonus_hours
            END;

        for _row in (
            select
                distinct employee_id,
                SUM(work_hours) OVER (PARTITION BY employee_id, project_id) as total_work_hours
            from logs where project_id = _id
        )
        loop
            insert into logs (employee_id, project_id, work_date, work_hours)
            values (_row.employee_id, _id, current_date, _actual_bonus_hours);
        end loop;

        update projects set is_active = false where id = _id;
    end if;
end
$$;

--Задание 4
create or replace procedure log_work(_employee_id uuid, _project_id uuid, _day date, _work_hours integer)
    language plpgsql
as $$
declare
    _max_hour_without_review integer := 16;
    _max_interval_without_review interval := '7 days';
    _required_review bool;
begin
    if (select exists(select 1 from projects where id = _project_id)) = false
    then raise exception 'project not found';
    end if;

    if (select exists(select 1 from employees where id = _employee_id)) = false
    then raise exception 'employee not found';
    end if;

    if (select is_active from projects where id = _project_id) = false
    then raise exception 'project is already closed';
    end if;

    if _work_hours <1 or _work_hours > 24
    then raise exception 'work hours must be between [1 and 8]';
    end if;

    select into _required_review
        CASE
            when
                    _work_hours > _max_hour_without_review
                    or _day > (select current_date)
                    or _day < ((select current_date) - _max_interval_without_review)
                then true
            else false
            END;

    insert into logs (employee_id, project_id, work_date, work_hours, required_review)
    values (_employee_id, _project_id, _day, _work_hours, _required_review);
end
$$;

--Задание 5
create table if not exists employee_rate_history(
    id              serial primary key,
    employee_id     uuid not null,
    rate            integer not null,
    from_date       date not null default '2020-12-26',
    constraint fk_employee_id foreign key (employee_id) references employees(id),
    constraint rate_must_be_positive check (rate > 0),
    constraint from_date_must_be_later_than_2020_12_25 check (from_date >= '2020-12-26')
);

create or replace function save_employee_rate_history()
    returns trigger
    language plpgsql
as $$
begin
    insert into employee_rate_history (employee_id, rate, from_date)
    values (new.id, new.rate, CURRENT_DATE);
    return new;
END
$$;

create or replace trigger change_employee_rate
    after insert or update on employees
    for each row
execute function save_employee_rate_history();

insert into employee_rate_history (employee_id, rate)
select id, rate from employees;

--Задание 6
create or replace function best_project_workers(_project_id uuid)
    returns table (
    employee text,
    work_hours bigint
    )
    language plpgsql
as $$
begin
    if (select exists(select 1 from projects where id = _project_id)) = false
    then raise exception 'project not found';
    end if;

    return query
        select
            distinct em.name as name,
                     SUM(lg.work_hours) OVER (PARTITION BY lg.employee_id, lg.project_id) as work_hours
        from logs lg
        left join employees em on lg.employee_id = em.id
        where project_id = _project_id
        order by work_hours desc
        limit 3;
END
$$;

--Задание 7*
create or replace function calculate_month_salary(_month_begin date, _month_end date)
    returns table (
    id uuid,
    worked_hours bigint,
    salary numeric(9,2)
    )
language plpgsql
as $$
declare
    _normal_hours integer := 160;
    _overtime_coefficient numeric := 1.25;
    _row record;
begin
    if extract(month from _month_begin) != extract(month from _month_end)
    then raise exception 'month_begin and month_end must be in the same month';
    end if;

    if extract(year from _month_begin) != extract(year from _month_end)
    then raise exception 'month_begin and month_end must be in the same year';
    end if;

    if _month_begin > _month_end
    then raise exception 'month_begin must be before month_end';
    end if;

    if _month_end != (date_trunc('month', _month_end) + interval '1 month - 1 day')::date
    then raise exception '_month_end is not actual month end';
    end if;

    if _month_begin != (date_trunc('month', (_month_begin- interval '1 month')) + interval '1 month')::date
    then raise exception '_month_begin is not actual month begin';
    end if;

    for _row in (
    select
        distinct lg.employee_id as id,
                 em.name as name,
                 lg.required_review as required_review
                 from logs lg
                 left join employees em on lg.employee_id = em.id
                 where work_date >= _month_begin and work_date <= _month_end and is_paid = false and required_review = true) loop
        raise notice 'Warning! Employee with id = %, name = % hours must be reviewed!', _row.id, _row.name;
    end loop;

    return query
        with worked_hours as (
            select
                employee_id, project_id, work_hours
            from logs where work_date >= _month_begin and work_date <= _month_end and required_review = false and is_paid = false
        )
        select
            wh.employee_id as id,
            SUM(wh.work_hours) as worked_hours,
            case
                when SUM(wh.work_hours) > _normal_hours
                    then (_normal_hours * em.rate)::numeric(9,2) +
                         ((SUM(wh.work_hours) - _normal_hours) * em.rate * _overtime_coefficient)::numeric(9,2)
                else (SUM(wh.work_hours) * em.rate)::numeric(9,2)
            end as salary
        from worked_hours wh
        left join employees em on wh.employee_id = em.id
        group by wh.employee_id, em.rate
        order by worked_hours desc;
END
$$;
