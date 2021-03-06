/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Assert;
import org.junit.Test;

public class TestGenericUDFOPMinus extends AbstractTestGenericUDFOPNumeric {

  private static final double EPSILON = 1E-6;

  @Test
  public void testByteMinusShort() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    ByteWritable left = new ByteWritable((byte) 4);
    ShortWritable right = new ShortWritable((short) 6);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableByteObjectInspector,
        PrimitiveObjectInspectorFactory.writableShortObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.shortTypeInfo);
    ShortWritable res = (ShortWritable) udf.evaluate(args);
    Assert.assertEquals(-2, res.get());
  }

  @Test
  public void testVarcharMinusInt() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    HiveVarcharWritable left = new HiveVarcharWritable();
    left.set("123");
    IntWritable right = new IntWritable(456);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector,
        PrimitiveObjectInspectorFactory.writableIntObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.doubleTypeInfo);
    DoubleWritable res = (DoubleWritable) udf.evaluate(args);
    Assert.assertEquals(-333.0, res.get(), EPSILON);
  }

  @Test
  public void testDoubleMinusLong() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    // Int
    DoubleWritable left = new DoubleWritable(4.5);
    LongWritable right = new LongWritable(10);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        PrimitiveObjectInspectorFactory.writableLongObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.doubleTypeInfo, oi.getTypeInfo());
    DoubleWritable res = (DoubleWritable) udf.evaluate(args);
    Assert.assertEquals(-5.5, res.get(), EPSILON);
  }

  @Test
  public void testLongMinusDecimal() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    LongWritable left = new LongWritable(104);
    HiveDecimalWritable right = new HiveDecimalWritable(HiveDecimal.create("234.97"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(9, 4))
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(24,4), oi.getTypeInfo());
    HiveDecimalWritable res = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertEquals(HiveDecimal.create("-130.97"), res.getHiveDecimal());
  }

  @Test
  public void testFloatMinusFloat() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    FloatWritable f1 = new FloatWritable(4.5f);
    FloatWritable f2 = new FloatWritable(0.0f);
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(f1),
        new DeferredJavaObject(f2),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(oi.getTypeInfo(), TypeInfoFactory.floatTypeInfo);
    FloatWritable res = (FloatWritable) udf.evaluate(args);
    Assert.assertEquals(4.5, res.get(), EPSILON);
  }

  @Test
  public void testDouleMinusDecimal() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    DoubleWritable left = new DoubleWritable(74.52);
    HiveDecimalWritable right = new HiveDecimalWritable(HiveDecimal.create("234.97"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2))
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.doubleTypeInfo, oi.getTypeInfo());
    DoubleWritable res = (DoubleWritable) udf.evaluate(args);
    Assert.assertEquals(-160.45, res.get(), EPSILON);
  }

  @Test
  public void testDecimalMinusDecimal() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    HiveDecimalWritable left = new HiveDecimalWritable(HiveDecimal.create("14.5"));
    HiveDecimalWritable right = new HiveDecimalWritable(HiveDecimal.create("234.97"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(3, 1)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2))
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(6,2), oi.getTypeInfo());
    HiveDecimalWritable res = (HiveDecimalWritable) udf.evaluate(args);
    Assert.assertEquals( HiveDecimal.create("-220.47"), res.getHiveDecimal());
  }

  @Test
  public void testDecimalMinusDecimalSameParams() throws HiveException {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(5, 2))
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(6, 2), oi.getTypeInfo());
  }

  @Test
  public void testReturnTypeBackwardCompat() throws Exception {
    // Disable ansi sql arithmetic changes
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "0.12");

    verifyReturnType(new GenericUDFOPMinus(), "int", "int", "int");
    verifyReturnType(new GenericUDFOPMinus(), "int", "float", "float");
    verifyReturnType(new GenericUDFOPMinus(), "int", "double", "double");
    verifyReturnType(new GenericUDFOPMinus(), "int", "decimal(10,2)", "decimal(13,2)");

    verifyReturnType(new GenericUDFOPMinus(), "float", "float", "float");
    verifyReturnType(new GenericUDFOPMinus(), "float", "double", "double");
    verifyReturnType(new GenericUDFOPMinus(), "float", "decimal(10,2)", "float");

    verifyReturnType(new GenericUDFOPMinus(), "double", "double", "double");
    verifyReturnType(new GenericUDFOPMinus(), "double", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPMinus(), "decimal(10,2)", "decimal(10,2)", "decimal(11,2)");

    // Most tests are done with ANSI SQL mode enabled, set it back to true
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");
  }

  @Test
  public void testReturnTypeAnsiSql() throws Exception {
    SessionState.get().getConf().setVar(HiveConf.ConfVars.HIVE_COMPAT, "latest");

    verifyReturnType(new GenericUDFOPMinus(), "int", "int", "int");
    verifyReturnType(new GenericUDFOPMinus(), "int", "float", "float");
    verifyReturnType(new GenericUDFOPMinus(), "int", "double", "double");
    verifyReturnType(new GenericUDFOPMinus(), "int", "decimal(10,2)", "decimal(13,2)");

    verifyReturnType(new GenericUDFOPMinus(), "float", "float", "float");
    verifyReturnType(new GenericUDFOPMinus(), "float", "double", "double");
    verifyReturnType(new GenericUDFOPMinus(), "float", "decimal(10,2)", "float");

    verifyReturnType(new GenericUDFOPMinus(), "double", "double", "double");
    verifyReturnType(new GenericUDFOPMinus(), "double", "decimal(10,2)", "double");

    verifyReturnType(new GenericUDFOPMinus(), "decimal(10,2)", "decimal(10,2)", "decimal(11,2)");
  }

  @Test
  public void testIntervalYearMonthMinusIntervalYearMonth() throws Exception {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    HiveIntervalYearMonthWritable left =
        new HiveIntervalYearMonthWritable(HiveIntervalYearMonth.valueOf("3-1"));
    HiveIntervalYearMonthWritable right =
        new HiveIntervalYearMonthWritable(HiveIntervalYearMonth.valueOf("1-2"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.intervalYearMonthTypeInfo, oi.getTypeInfo());
    HiveIntervalYearMonthWritable res = (HiveIntervalYearMonthWritable) udf.evaluate(args);
    Assert.assertEquals(HiveIntervalYearMonth.valueOf("1-11"), res.getHiveIntervalYearMonth());
  }


  @Test
  public void testDateMinusIntervalYearMonth() throws Exception {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    DateWritableV2 left =
        new DateWritableV2(Date.valueOf("2004-02-15"));
    HiveIntervalYearMonthWritable right =
        new HiveIntervalYearMonthWritable(HiveIntervalYearMonth.valueOf("2-8"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableDateObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.dateTypeInfo, oi.getTypeInfo());
    DateWritableV2 res = (DateWritableV2) udf.evaluate(args);
    Assert.assertEquals(Date.valueOf("2001-06-15"), res.get());
  }

  @Test
  public void testTimestampMinusIntervalYearMonth() throws Exception {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    TimestampWritableV2 left =
        new TimestampWritableV2(Timestamp.valueOf("2004-01-15 01:02:03.123456789"));
    HiveIntervalYearMonthWritable right =
        new HiveIntervalYearMonthWritable(HiveIntervalYearMonth.valueOf("2-2"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo, oi.getTypeInfo());
    TimestampWritableV2 res = (TimestampWritableV2) udf.evaluate(args);
    Assert.assertEquals(Timestamp.valueOf("2001-11-15 01:02:03.123456789"), res.getTimestamp());
  }

  @Test
  public void testIntervalDayTimeMinusIntervalDayTime() throws Exception {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    HiveIntervalDayTimeWritable left =
        new HiveIntervalDayTimeWritable(HiveIntervalDayTime.valueOf("2 2:3:4.567"));
    HiveIntervalDayTimeWritable right =
        new HiveIntervalDayTimeWritable(HiveIntervalDayTime.valueOf("1 2:3:4"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.intervalDayTimeTypeInfo, oi.getTypeInfo());
    HiveIntervalDayTimeWritable res = (HiveIntervalDayTimeWritable) udf.evaluate(args);
    Assert.assertEquals(HiveIntervalDayTime.valueOf("1 0:0:0.567"), res.getHiveIntervalDayTime());
  }

  @Test
  public void testTimestampMinusIntervalDayTime() throws Exception {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    TimestampWritableV2 left =
        new TimestampWritableV2(Timestamp.valueOf("2001-01-02 2:3:4.567"));
    HiveIntervalDayTimeWritable right =
        new HiveIntervalDayTimeWritable(HiveIntervalDayTime.valueOf("1 2:3:4.567"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo, oi.getTypeInfo());
    TimestampWritableV2 res = (TimestampWritableV2) udf.evaluate(args);
    Assert.assertEquals(Timestamp.valueOf("2001-01-01 00:00:00"), res.getTimestamp());
  }

  @Test
  public void testDateMinusIntervalDayTime() throws Exception {
    GenericUDFOPMinus udf = new GenericUDFOPMinus();

    DateWritableV2 left =
        new DateWritableV2(Date.valueOf("2001-01-01"));
    HiveIntervalDayTimeWritable right =
        new HiveIntervalDayTimeWritable(HiveIntervalDayTime.valueOf("1 0:0:0.555"));
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.writableDateObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector
    };
    DeferredObject[] args = {
        new DeferredJavaObject(left),
        new DeferredJavaObject(right),
    };

    PrimitiveObjectInspector oi = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    Assert.assertEquals(TypeInfoFactory.timestampTypeInfo, oi.getTypeInfo());
    TimestampWritableV2 res = (TimestampWritableV2) udf.evaluate(args);
    Assert.assertEquals(Timestamp.valueOf("2000-12-30 23:59:59.445"), res.getTimestamp());
  }
}
