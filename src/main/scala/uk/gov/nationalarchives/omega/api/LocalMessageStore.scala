package uk.gov.nationalarchives.omega.api

import cats.effect.{IO, Resource}
import com.fasterxml.uuid.{EthernetAddress, Generators}
import jms4s.jms.JmsMessage

import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.UUID

object LocalMessageStore {
  type PersistentMessageId = UUID
}

class LocalMessageStore(folder: Path) {
  private val uuidGenerator = Generators.timeBasedGenerator(EthernetAddress.fromInterface)

  import LocalMessageStore.PersistentMessageId

  def persistMessage(message: JmsMessage): IO[PersistentMessageId] = {
    def newMessageFileId(): IO[PersistentMessageId] = {
      IO.delay {
        uuidGenerator.generate()
      }
    }

    def openNewMessageFile(messageId: PersistentMessageId): IO[ByteChannel] = {
      IO.blocking {
        val path = folder.resolve(s"${messageId}.msg")
        Files.newByteChannel(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.DSYNC)
      }
    }

    def closeMessageFile(byteChannel: ByteChannel): IO[Unit] = {
      IO.blocking {
        byteChannel.close()
      }
    }

    newMessageFileId.flatMap { persistentMessageId: UUID =>
      message.asTextF[IO].flatMap { messageText: String =>
        Resource.make(openNewMessageFile(persistentMessageId))(closeMessageFile).use { messageFile =>
          IO.blocking {
            val buffer = ByteBuffer.wrap(messageText.getBytes(UTF_8))
            messageFile.write(buffer)
            persistentMessageId
          }
        }
      }
    }
  }
}
