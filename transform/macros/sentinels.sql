{#
  GSPT writes its non-answers into the value column: `unknown`, `>0`, `N/A`.

  Collapsing those to NULL loses a real distinction. "The tracker reports this plant's
  capacity as unknown" and "the tracker has no row for this plant's capacity" are
  different facts, and only the first one tells you the compiler looked. So every
  sentinel-bearing column becomes two: a numeric value, and a status saying which of
  the three states the source was in.
#}

{% macro sentinel_status(column) %}
    case
        when {{ column }} is null then 'absent'
        when lower(trim({{ column }})) in ('unknown', 'n/a', 'na', 'none', '-') then 'unknown'
        when trim({{ column }}) like '>%' or trim({{ column }}) like '<%' then 'bounded'
        when try_cast(replace(trim({{ column }}), ',', '') as double) is not null then 'reported'
        else 'unparsed'
    end
{% endmacro %}

{% macro sentinel_value(column) %}
    try_cast(replace(trim({{ column }}), ',', '') as double)
{% endmacro %}

{#
  A four-digit year, or null. PLANTGEN writes 9999 for "unknown" in PlantStartYr; GSPT
  writes `unknown`, `N/A` and `>2019` into its date columns. A 9999 left alone reads as
  a real year and turns an absence into a disagreement.
#}
{% macro plausible_year(column) %}
    case
        when try_cast(regexp_extract(cast({{ column }} as varchar), '(\d{4})', 1) as integer)
             between 1700 and 2030
        then try_cast(regexp_extract(cast({{ column }} as varchar), '(\d{4})', 1) as integer)
    end
{% endmacro %}

{#
  Owner names must be compared on their normalised form. Diffing the raw strings
  measures formatting churn, not ownership change: "Nucor Corp." and "Nucor Corp"
  are the same owner and a raw diff would report a transfer between them.

  The 48 rows carrying an encoding-damaged owner (`Standard Steel ? Burnham`,
  `North American H?an?`) have no intact counterpart anywhere -- the vintage that holds
  owners is the vintage that was damaged. The `?` is stripped like any other
  punctuation so those rows still normalise to a stable key; they are flagged
  separately rather than repaired, because nothing can repair them.
#}
{% macro normalize_owner(column) %}
    nullif(
        trim(
            regexp_replace(
                regexp_replace(
                    lower(cast({{ column }} as varchar)),
                    '\b(inc|incorporated|corp|corporation|co|company|ltd|limited|llc|lp|plc|group|holdings?|sa|ag|nv|bv)\b',
                    '', 'g'
                ),
                '[^a-z0-9]+', ' ', 'g'
            )
        ),
        ''
    )
{% endmacro %}
