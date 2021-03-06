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

package io.prestosql.tpcds;

import io.prestosql.tpcds.Parallel.ChunkBoundaries;
import org.testng.annotations.Test;

import static io.prestosql.tpcds.GeneratorAssertions.assertPartialMD5;
import static io.prestosql.tpcds.Parallel.splitWork;
import static io.prestosql.tpcds.Session.getDefaultSession;
import static io.prestosql.tpcds.Table.INVENTORY;

public class TestInventoryGenerator
{
    private static final Session TEST_SESSION = getDefaultSession().withTable(INVENTORY);

    // See the comment in CallCenterGeneratorTest for an explanation on the purpose of this test.
    @Test
    public void testScaleFactor0_01()
    {
        Session session = TEST_SESSION.withScale(0.01);
        assertPartialMD5(1, session.getScaling().getRowCount(INVENTORY), INVENTORY, session, "4b30d1ba8ec5743221651fcd7b3c1a57");
    }

    @Test
    public void testScaleFactor1()
    {
        Session session = TEST_SESSION.withScale(1);
        assertPartialMD5(1, session.getScaling().getRowCount(INVENTORY), INVENTORY, session, "cfefc8724693ec9149f1d5b345fcecc2");
    }

    @Test
    public void testScaleFactor10()
    {
        Session session = TEST_SESSION.withScale(10).withParallelism(1000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "09d20281601b878635e4582f0357f6b2");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "551caeef38e8e85f87be60960b9e5749");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "e7b3e0095e40383a2e27bcce55ec5463");
    }

    @Test
    public void testScaleFactor100()
    {
        Session session = TEST_SESSION.withScale(100).withParallelism(10000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "ec5a8e36b6d1fd39a083f0e1f0d55412");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "9f020ad4cb016a9e7ad1ff5fb1cabb98");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "5d332043c07d5edbdecd47da5fe81d38");
    }

    @Test
    public void testScaleFactor300()
    {
        Session session = TEST_SESSION.withScale(300).withParallelism(30000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "5579d7480fc3cd36edc1a4567c87fb00");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "fb1804accc3ed79f635c288f2d587929");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "167821294764590bf48416784236bdd7");
    }

    @Test
    public void testScaleFactor1000()
    {
        Session session = TEST_SESSION.withScale(1000).withParallelism(100000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "edbcec71eba174c66171e46b0b206af0");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "ec513ab116d243ed80123926a8a0ceb1");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "0dbbbafd454d138616b9ad2659fadd53");
    }

    @Test
    public void testScaleFactor3000()
    {
        Session session = TEST_SESSION.withScale(3000).withParallelism(300000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "7a6c07f0a2da6a0656e1f07fcb39f92d");

        session = session.withChunkNumber(10000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "dc920cdbf3ee628483f58b69978d0843");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "63b570fa604eff367803440bbacfbcac");
    }

    @Test
    public void testScaleFactor10000()
    {
        Session session = TEST_SESSION.withScale(10000).withParallelism(1000000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "f80ee6f95e89d62c2c39161b9a13eab0");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "1a6c5fcab171087d94dfc3fe98a910a4");

        session = session.withChunkNumber(1000000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "a2dfe28d2f21cdcedb59a225c246869e");
    }

    @Test
    public void testScaleFactor30000()
    {
        Session session = TEST_SESSION.withScale(30000).withParallelism(3000000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "fba79dbabd532aeea662bd51cc050d6f");

        session = session.withChunkNumber(100000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "a08744a9b84de4cce5450fc8c7f15c17");

        session = session.withChunkNumber(1000000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "14ab5109d8b6f35d60de24b090bc5476");
    }

    @Test
    public void testScaleFactor100000()
    {
        Session session = TEST_SESSION.withScale(100000).withParallelism(10000000).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "28d52191c57ebf202ead858ee3d7d801");

        session = session.withChunkNumber(1000000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "546d81dd9cb0dbfd260c557a79cecfb2");

        session = session.withChunkNumber(10000000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "fccf87986ccc92d75c751de512876925");
    }

    @Test
    public void testUndefinedScale()
    {
        Session session = TEST_SESSION.withScale(15).withParallelism(1500).withChunkNumber(1);
        ChunkBoundaries chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "3b8c9aefcfab2b7cf9e5e0c4e40e4df9");

        session = session.withChunkNumber(100);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "d93a9573750a08e323ae2e7a4e79c77f");

        session = session.withChunkNumber(1000);
        chunkBoundaries = splitWork(INVENTORY, session);
        assertPartialMD5(chunkBoundaries.getFirstRow(), chunkBoundaries.getLastRow(), INVENTORY, session, "ef5f4fcb2379a30160fc143dbe9180d0");
    }
}
