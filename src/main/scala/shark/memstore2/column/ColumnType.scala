/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.memstore2.column

import java.io.DataInput
import java.io.DataOutput

import java.nio.ByteBuffer
import java.sql.Timestamp

import org.apache.hadoop.hive.serde2.ByteStream
import org.apache.hadoop.hive.serde2.`lazy`.{ByteArrayRef, LazyBinary}
import org.apache.hadoop.hive.serde2.io.{TimestampWritable, ShortWritable, ByteWritable, DoubleWritable}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io._


/**
 * @param typeID A unique ID representing the type.
 * @param defaultSize Default size in bytes for one element of type T (e.g. Int = 4).
 * @tparam T Scala data type for the column.
 * @tparam V Writable data type for the column.
 */
sealed abstract class ColumnType[T : ClassManifest, V : ClassManifest](
    val typeID: Int, val defaultSize: Int) {

  /**
   * Scala class manifest. Can be used to create primitive arrays and hash tables.
   */
  def scalaManifest: ClassManifest[T] = classManifest[T]

  /**
   * Scala class manifest for the writable type. Can be used to create primitive arrays and
   * hash tables.
   */
  def writableManifest: ClassManifest[V] = classManifest[V]

  /**
   * Extract a value out of the buffer at the buffer's current position.
   */
  def extract(buffer: ByteBuffer): T

  /**
   * Append the given value v of type T into the given ByteBuffer.
   */
  def append(v: T, buffer: ByteBuffer)

  /**
   * Return the Scala data representation of the given object, using an object inspector.
   */
  def get(o: Object, oi: ObjectInspector): T

  /**
   * Return the size of the value. This is used to calculate the size of variable length types
   * such as byte arrays and strings.
   */
  def actualSize(v: T): Int = defaultSize

  /**
   * Extract a value out of the buffer at the buffer's current position, and put it in the writable
   * object. This is used as an optimization to reduce the temporary objects created, since the
   * writable object can be reused.
   */
  def extractInto(buffer: ByteBuffer, writable: V)

  /**
   * Create a new writable object corresponding to this type.
   */
  def newWritable(): V

  /**
   * Create a duplicated copy of the value.
   */
  def clone(v: T): T = v
}


object INT extends ColumnType[Int, IntWritable](0, 4) {

  override def append(v: Int, buffer: ByteBuffer) {
    buffer.putInt(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getInt()
  }

  override def get(o: Object, oi: ObjectInspector): Int = {
    oi.asInstanceOf[IntObjectInspector].get(o)
  }

  override def extractInto(buffer: ByteBuffer, writable: IntWritable) {
    writable.set(extract(buffer))
  }

  override def newWritable() = new IntWritable
}


object LONG extends ColumnType[Long, LongWritable](1, 8) {

  override def append(v: Long, buffer: ByteBuffer) {
    buffer.putLong(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getLong()
  }

  override def get(o: Object, oi: ObjectInspector): Long = {
    oi.asInstanceOf[LongObjectInspector].get(o)
  }

  override def extractInto(buffer: ByteBuffer, writable: LongWritable) {
    writable.set(extract(buffer))
  }

  override def newWritable() = new LongWritable
}


object FLOAT extends ColumnType[Float, FloatWritable](2, 4) {

  override def append(v: Float, buffer: ByteBuffer) {
    buffer.putFloat(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getFloat()
  }

  override def get(o: Object, oi: ObjectInspector): Float = {
    oi.asInstanceOf[FloatObjectInspector].get(o)
  }

  override def extractInto(buffer: ByteBuffer, writable: FloatWritable) {
    writable.set(extract(buffer))
  }

  override def newWritable() = new FloatWritable
}


object DOUBLE extends ColumnType[Double, DoubleWritable](3, 8) {

  override def append(v: Double, buffer: ByteBuffer) {
    buffer.putDouble(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getDouble()
  }

  override def get(o: Object, oi: ObjectInspector): Double = {
    oi.asInstanceOf[DoubleObjectInspector].get(o)
  }

  override def extractInto(buffer: ByteBuffer, writable: DoubleWritable) {
    writable.set(extract(buffer))
  }

  override def newWritable() = new DoubleWritable
}


object BOOLEAN extends ColumnType[Boolean, BooleanWritable](4, 1) {

  override def append(v: Boolean, buffer: ByteBuffer) {
    buffer.put(if (v) 1.toByte else 0.toByte)
  }

  override def extract(buffer: ByteBuffer) = {
    if (buffer.get() == 1) true else false
  }

  override def get(o: Object, oi: ObjectInspector): Boolean = {
    oi.asInstanceOf[BooleanObjectInspector].get(o)
  }

  override def extractInto(buffer: ByteBuffer, writable: BooleanWritable) {
    writable.set(extract(buffer))
  }

  override def newWritable() = new BooleanWritable
}


object BYTE extends ColumnType[Byte, ByteWritable](5, 1) {

  override def append(v: Byte, buffer: ByteBuffer) {
    buffer.put(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.get()
  }
  override def get(o: Object, oi: ObjectInspector): Byte = {
    oi.asInstanceOf[ByteObjectInspector].get(o)
  }

  override def extractInto(buffer: ByteBuffer, writable: ByteWritable) {
    writable.set(extract(buffer))
  }

  override def newWritable() = new ByteWritable
}


object SHORT extends ColumnType[Short, ShortWritable](6, 2) {

  override def append(v: Short, buffer: ByteBuffer) {
    buffer.putShort(v)
  }

  override def extract(buffer: ByteBuffer) = {
    buffer.getShort()
  }

  override def get(o: Object, oi: ObjectInspector): Short = {
    oi.asInstanceOf[ShortObjectInspector].get(o)
  }

  def extractInto(buffer: ByteBuffer, writable: ShortWritable) {
    writable.set(extract(buffer))
  }

  def newWritable() = new ShortWritable
}


object VOID extends ColumnType[Void, NullWritable](7, 0) {

  override def append(v: Void, buffer: ByteBuffer) {}

  override def extract(buffer: ByteBuffer) = {
    throw new UnsupportedOperationException()
  }

  override def get(o: Object, oi: ObjectInspector) = null

  override def extractInto(buffer: ByteBuffer, writable: NullWritable) {}

  override def newWritable() = NullWritable.get
}


object STRING extends ColumnType[Text, Text](8, 8) {

  private val _bytesFld = {
    val f = classOf[Text].getDeclaredField("bytes")
    f.setAccessible(true)
    f
  }

  private val _lengthFld = {
    val f = classOf[Text].getDeclaredField("length")
    f.setAccessible(true)
    f
  }

  override def append(v: Text, buffer: ByteBuffer) {
    val length = v.getLength()
    buffer.putInt(length)
    buffer.put(v.getBytes(), 0, length)
  }

  override def extract(buffer: ByteBuffer) = {
    val t = new Text()
    extractInto(buffer, t)
    t
  }

  override def get(o: Object, oi: ObjectInspector): Text = {
    oi.asInstanceOf[StringObjectInspector].getPrimitiveWritableObject(o)
  }

  override def actualSize(v: Text) = v.getLength() + 4

  override def extractInto(buffer: ByteBuffer, writable: Text) {
    val length = buffer.getInt()
    var b = _bytesFld.get(writable).asInstanceOf[Array[Byte]]
    if (b == null || b.length < length) {
      b = new Array[Byte](length)
      _bytesFld.set(writable, b)
    }
    buffer.get(b, 0, length)
    _lengthFld.set(writable, length)
  }

  override def newWritable() = new Text

  override def clone(v: Text) = {
    val t = new Text()
    t.set(v)
    t
  }
}


object TIMESTAMP extends ColumnType[Timestamp, TimestampWritable](9, 12) {

  override def append(v: Timestamp, buffer: ByteBuffer) {
    buffer.putLong(v.getTime())
    buffer.putInt(v.getNanos())
  }

  override def extract(buffer: ByteBuffer) = {
    val ts = new Timestamp(0)
    ts.setTime(buffer.getLong())
    ts.setNanos(buffer.getInt())
    ts
  }

  override def get(o: Object, oi: ObjectInspector): Timestamp = {
    oi.asInstanceOf[TimestampObjectInspector].getPrimitiveJavaObject(o)
  }

  override def extractInto(buffer: ByteBuffer, writable: TimestampWritable) {
    writable.set(extract(buffer))
  }

  override def newWritable() = new TimestampWritable
}


object BINARY extends ColumnType[BytesWritable, BytesWritable](10, 16) {

  private val _bytesFld = {
    val f = classOf[BytesWritable].getDeclaredField("bytes")
    f.setAccessible(true)
    f
  }

  private val _lengthFld = {
    val f = classOf[BytesWritable].getDeclaredField("size")
    f.setAccessible(true)
    f
  }

  override def append(v: BytesWritable, buffer: ByteBuffer) {
    val length = v.getLength()
    buffer.putInt(length)
    buffer.put(v.getBytes(), 0, length)
  }

  override def extract(buffer: ByteBuffer) = {
    throw new UnsupportedOperationException()
  }

  override def get(o: Object, oi: ObjectInspector): BytesWritable = {
    o match {
      case lb: LazyBinary => lb.getWritableObject()
      case b: BytesWritable => b
      case _ => throw new UnsupportedOperationException("Unknown binary type " + oi)
    }
  }

  override def extractInto(buffer: ByteBuffer, writable: BytesWritable) {
    val length = buffer.getInt()
    var b = _bytesFld.get(writable).asInstanceOf[Array[Byte]]
    if (b == null || b.length < length) {
      b = new Array[Byte](length)
      _bytesFld.set(writable, b)
    }
    buffer.get(b, 0, length)
    _lengthFld.set(writable, length)
  }

  override def newWritable() = new BytesWritable
  
  override def actualSize(v: BytesWritable) = v.getLength() + 4
}


object GENERIC extends ColumnType[ByteStream.Output, ByteArrayRef](11, 16) {

  override def append(v: ByteStream.Output, buffer: ByteBuffer) {
    val length = v.getCount()
    buffer.putInt(length)
    buffer.put(v.getData(), 0, length)
  }

  override def extract(buffer: ByteBuffer) = {
    throw new UnsupportedOperationException()
  }

  override def get(o: Object, oi: ObjectInspector) = {
    o.asInstanceOf[ByteStream.Output]
  }

  override def extractInto(buffer: ByteBuffer, writable: ByteArrayRef) {
    val length = buffer.getInt()
    val a = new Array[Byte](length)
    buffer.get(a, 0, length)
    writable.setData(a)
  }

  override def newWritable() = new ByteArrayRef
}

case class NEWBOOLEAN (
  var bitPos: Byte = 0,
  var values : Int = 0,
  var initialized : Boolean = false) extends ColumnType[Boolean, NewBooleanWritable](NEWBOOLEAN.typeID, NEWBOOLEAN.defaultSize) {

  //var bitPos: Byte = 0
  //var values: Int = 0
  //var initialized = false

  override def append(v: Boolean, buffer: ByteBuffer) {
   if (bitPos < NewBooleanWritable.MAX_NUMBER_OF_PACKED_BOOLEANS) {
     updateValues(v)
     incrPos()
   } else {
     buffer.put(bitPos)
     buffer.putInt(values)
     bitPos = 0
     values = 0
   }
  }

  override def extract(buffer: ByteBuffer) = {
    if (bitPos < NewBooleanWritable.MAX_NUMBER_OF_PACKED_BOOLEANS && initialized) {
      val v = currentValue
      incrPos()
      v
    } else {
      bitPos = buffer.get()
      values = buffer.getInt()
      initialized = true
      currentValue
    }
  }

  override def get(o: Object, oi: ObjectInspector): Boolean = {
    val v = currentValue
    incrPos()
    v
  }

  override def extractInto(buffer: ByteBuffer, writable: NewBooleanWritable) {
    writable.set(extract(buffer), bitPos)
  }

  override def newWritable() = new NewBooleanWritable(0, 0)

  private def updateValues(v: Boolean) {
    if(v) {
      values |= 1 << bitPos
    } else {
      values &= 0 << bitPos
    }
  }

  private val currentValue = (values & (1 << bitPos)) > 0

  private def incrPos() = {
    bitPos = (bitPos.asInstanceOf[Int] + 1).asInstanceOf[Byte]
  }
}

object NEWBOOLEAN {
  val typeID : Int = 12
  val defaultSize: Int = 8

  def apply() = new NEWBOOLEAN()
}

/**
 * ByteWritable.
 *
 */

object NewBooleanWritable {
  val MAX_NUMBER_OF_PACKED_BOOLEANS = 30
  val BOOLEAN_VALUES_OFFSET = 5
}

class NewBooleanWritable(var bitPos: Byte, var values: Int) extends WritableComparable[NewBooleanWritable] {

  def write(out: DataOutput) {
    out.writeByte(bitPos)
    out.writeInt(values)
  }

  def readFields(in: DataInput) {
    bitPos = in.readByte()

    assert(bitPos < NewBooleanWritable.MAX_NUMBER_OF_PACKED_BOOLEANS)

    values = in.readInt()
  }

  def set(v: Boolean, pos: Byte) {
    if (v) {
      values |= 1 << pos
    } else {
      values &= 0 << pos
    }
  }

  /** Compares two NewBooleanWritables. */
  override def compareTo(o: NewBooleanWritable): Int = {
    val other = o.asInstanceOf[NewBooleanWritable]

    assert(bitPos < NewBooleanWritable.BOOLEAN_VALUES_OFFSET)
    assert(other.bitPos < NewBooleanWritable.BOOLEAN_VALUES_OFFSET)

    val pos_difference = bitPos - other.bitPos
    val val_difference = values - other.values

    (val_difference << NewBooleanWritable.BOOLEAN_VALUES_OFFSET) + pos_difference
  }

  override def equals(o: Any) : Boolean = {
    if (o == null || o.getClass.getName != classOf[NewBooleanWritable]) {
      return false
    }
    val other = o.asInstanceOf[NewBooleanWritable]
    values == other.values && bitPos == other.bitPos
  }

  override def hashCode: Int = {
    (values << NewBooleanWritable.BOOLEAN_VALUES_OFFSET) + bitPos
  }

  override def toString: String = {
    val str = values.toBinaryString
    str + ", " + bitPos.toString
  }
}

