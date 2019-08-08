package test.org.apache.spark.sql.jdbc;

import java.sql.Timestamp;

public class CalciteTestData {

  public final Foo[] FOO = {
    new Foo("AAA", Timestamp.valueOf("2019-01-02 00:00:00")),
    new Foo("BBB", Timestamp.valueOf("2019-03-05 00:00:00")),
    new Foo("CCC", Timestamp.valueOf("2019-08-01 00:00:00"))
  };

  public static class Foo {
    public final String ID;
    public final java.sql.Timestamp TIMES;

    public Foo(String s, java.sql.Timestamp t) {
      this.ID = s;
      this.TIMES = t;
    }
  }
}
