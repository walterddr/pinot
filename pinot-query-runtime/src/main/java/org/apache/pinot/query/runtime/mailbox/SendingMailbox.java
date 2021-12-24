package org.apache.pinot.query.runtime.mailbox;

/**
 * Mailbox is used to send and receive data.
 *
 * Mailbox should be instantiated on both side of MailboxServer.
 *
 * @param <T> type of data carried over the mailbox.
 */
public interface SendingMailbox<T> {

  /**
   * get the unique identifier for the mailbox.
   *
   * @return Mailbox ID.
   */
  String getMailboxId();

  /**
   * send a data packet through the mailbox.
   * @param data
   * @throws UnsupportedOperationException
   */
  void send(T data) throws UnsupportedOperationException;
}
