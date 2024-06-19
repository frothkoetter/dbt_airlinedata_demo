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
{% test row_count_between(model, min_row_count, max_row_count) %}
    with row_count as (
        select count(*) as actual_row_count
        from {{ model }}
    )
    select
        case
            when actual_row_count between {{ min_row_count }} and {{ max_row_count }} then 1
            else 0
        end as test_passes
    from row_count
    where (case
				when actual_row_count between {{ min_row_count }} and {{ max_row_count }} then 1
				else 0 end ) = 0
{% endtest %}
