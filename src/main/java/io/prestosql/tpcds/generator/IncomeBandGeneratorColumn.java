/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.prestosql.tpcds.generator;

import io.prestosql.tpcds.Table;

import static io.prestosql.tpcds.Table.INCOME_BAND;

public enum IncomeBandGeneratorColumn
        implements GeneratorColumn
{
    IB_INCOME_BAND_ID(194, 1),
    IB_LOWER_BOUND(195, 1),
    IB_UPPER_BOUND(196, 1),
    IB_NULLS(197, 2);

    private final int globalColumnNumber;
    private final int seedsPerRow;

    IncomeBandGeneratorColumn(int globalColumnNumber, int seedsPerRow)
    {
        this.globalColumnNumber = globalColumnNumber;
        this.seedsPerRow = seedsPerRow;
    }

    @Override
    public Table getTable()
    {
        return INCOME_BAND;
    }

    @Override
    public int getGlobalColumnNumber()
    {
        return globalColumnNumber;
    }

    @Override
    public int getSeedsPerRow()
    {
        return seedsPerRow;
    }
}
