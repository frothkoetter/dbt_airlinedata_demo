{#
# /*
#  * Copyright 2022 Cloudera, Inc.
#  *
#  * Licensed under the Apache License, Version 2.0 (the "License");
#  * you may not use this file except in compliance with the License.
#  * You may obtain a copy of the License at
#  *
#  *   http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */
#}
{% test row_count_with_warning(model, expected_row_count) %}
    with row_count as (
        select count(*) as actual_row_count
        from {{ model }}
    ),
    thresholds as (
        select
            {{ expected_row_count }} as expected_row_count,
            {{ expected_row_count }} * 0.9 as lower_threshold,
            {{ expected_row_count }} * 1.1 as upper_threshold
    )
    select
        case
            when actual_row_count >= lower_threshold and actual_row_count <= upper_threshold then 1
            else 0
        end as test_passes
    from row_count, thresholds
    where case
        when actual_row_count >= lower_threshold and actual_row_count <= upper_threshold then 1
        else 0
    end = 0
{% endtest %}
