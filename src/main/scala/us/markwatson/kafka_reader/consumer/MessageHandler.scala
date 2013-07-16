package us.markwatson.kafka_reader.consumer

import java.nio.ByteBuffer

trait MessageHandler {
  protected var _message: ByteBuffer = null
  def message: ByteBuffer = _message
  def message_=(message: ByteBuffer) = {
    _message = message
  }

  override def toString: String = message.toString
}

class BinaryMessageHandler(ascii: Boolean = true) extends MessageHandler {
  val bytesPerLine = 16
  val bytesPerCol = 8

  override def toString: String = {
    val msgStr = new StringBuilder
    msgStr.append("\n")
    var lineAscii = new StringBuilder
    lineAscii.append("|")

    message.rewind()
    var pos = 0
    var haveAny = false
    while (message.hasRemaining) {
      haveAny = true
      pos = message.position()
      val byte = message.get()

      if (pos > 0) {
        if (pos % bytesPerLine == 0) {
          if (ascii) {
            lineAscii.append("|")
            msgStr.append("  ")
            msgStr.append(lineAscii)
            lineAscii = new StringBuilder
            lineAscii.append("|")
          }

          msgStr.append("\n")
        } else if (pos % bytesPerCol == 0) {
          msgStr.append(' ')
        }
      }

      if (pos % bytesPerLine == 0) {
        msgStr.append("%08X ".format(pos))
      }

      msgStr.append(" %02X".format(byte))
      lineAscii.append(byte.toChar)
    }
    if (haveAny && ascii) {
      lineAscii.append("|")
      val remainingBytes = bytesPerLine - (pos % bytesPerLine)
      val padding = if (remainingBytes > bytesPerCol) remainingBytes * 3
      else remainingBytes * 3 - 1
      for (i <- 0 until padding) {
        msgStr.append(" ")
      }
      msgStr.append(lineAscii)
    }

    msgStr.append("\n%08X ".format(pos))
    msgStr.toString()
  }
}

class SimpleMessageCoordinator(handler: MessageHandler) {
  def handle(m: ByteBuffer): String = {
    handler.message = m
    handler.toString
  }
}