package dev.forcecodes.kafka.reproduce;

public interface EventCorrelationIdUpdater<O> {

  void requestUpdate(O record);
}
