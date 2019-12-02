package com.x.farmer.bft.execute;

import java.util.List;

public interface MessageHandler {

    byte[] execute(List<byte[]> messages);
}
