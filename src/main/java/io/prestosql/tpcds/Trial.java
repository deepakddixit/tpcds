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

import java.util.List;

public class Trial {

  public static void main(String[] args) {
    Options options = new Options();
    options.directory = "/tmp/dir1";
    options.parallelism = 1;
    options.scale = 1.0d;
    options.table = "CALL_CENTER";
    options.overwrite = true;
    Session session = options.toSession();

    List<Table> tablesToGenerate = Table.getBaseTables();

//    for (int i = 1; i <= session.getParallelism(); i++) {
//      int chunkNumber = i;
//      new Thread(() -> {
//        TableGenerator tableGenerator = new TableGenerator(session.withChunkNumber(chunkNumber));
////        tablesToGenerate.forEach(tableGenerator::generateTable);
//        tableGenerator.generateTable(Table.CATALOG_RETURNS);
//      }).start();
//    }
    TableGenerator tableGenerator = new TableGenerator(session);
    SparkResultIterable sparkResultIterable = tableGenerator.generateTableAsItr(Table.CATALOG_RETURNS);

    long counter = 0;
    while (sparkResultIterable.hasNext()){
      List<Object> values = sparkResultIterable.next();
      System.out.println(values);
      counter++;
    }

    System.out.println("Total rows : "+ counter);


//    for (int i = 0; i < tablesToGenerate.length; i++) {
//      generator.generateTable(tablesToGenerate[i]);
//    }
  }
}
