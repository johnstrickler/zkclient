package org.I0Itec.zkclient;

import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.StaticHostProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Resolves addresses as-needed as opposed to its counterpart
 * {@link StaticHostProvider} which resolves all addresses on instantiation.
 */
public class LazyHostProvider implements HostProvider {

    private static final Logger log = LoggerFactory.getLogger(LazyHostProvider.class);
    private final InetSocketAddress[] unresolvedAddresses;
    private final List<InetSocketAddress> resolvedQueue = new ArrayList();

    private int currentIndex = -1;

    public LazyHostProvider(Collection<InetSocketAddress> serverAddresses) throws UnknownHostException {
        this.unresolvedAddresses = serverAddresses.toArray(new InetSocketAddress[]{});
    }

    public int size() {
        return this.unresolvedAddresses.length;
    }

    public void onConnected() {
        resolvedQueue.clear();
        currentIndex = 0;
    }

    public InetSocketAddress next(long spinDelay) {

        int tries = 0;

        // reload the queue with more resolved addresses?
        while(resolvedQueue.size() == 0 && tries++ < unresolvedAddresses.length) {

            // resolve more
            try {
                this.resolveAddressesToQueue();
            } catch (UnknownHostException e) {
                log.warn("No IP address found for server.", e);
            }
        }

        InetSocketAddress resolvedAddress = resolvedQueue.remove(0);

        boolean isLastPossibleAddress = (this.currentIndex == unresolvedAddresses.length - 1) && resolvedQueue.size() == 0;

        if(isLastPossibleAddress && spinDelay > 0L) {
            this.sleep(spinDelay);
        }

        return resolvedAddress;
    }

    private void sleep(long spinDelay) {

        try {
            Thread.sleep(spinDelay);
        } catch (InterruptedException e) {
            log.warn("Unexpected exception.", e);
        }
    }


    private void resolveAddressesToQueue() throws UnknownHostException {

        if(++this.currentIndex == this.unresolvedAddresses.length) {
            this.currentIndex = 0;
        }

        // resolve more
        InetSocketAddress unresolvedAddress = unresolvedAddresses[currentIndex];

        InetAddress ia = unresolvedAddress.getAddress();
        String hostName = ia != null ? ia.getHostAddress() : unresolvedAddress.getHostName();
        InetAddress[] resolvedAddresses = InetAddress.getAllByName(hostName);

        for(InetAddress resolvedAddress : Arrays.asList(resolvedAddresses)) {

            InetSocketAddress resolvedSocket = null;

            if(resolvedAddress.toString().startsWith("/") && resolvedAddress.getAddress() != null) {
                try {
                    resolvedSocket = new InetSocketAddress(InetAddress.getByAddress(unresolvedAddress.getHostName(), resolvedAddress.getAddress()), unresolvedAddress.getPort());
                } catch (UnknownHostException e) {
                    log.warn("Cannot resolve socket.", e);
                }
            } else {
                resolvedSocket = new InetSocketAddress(resolvedAddress.getHostAddress(), unresolvedAddress.getPort());
            }

            if(resolvedSocket != null) {
                this.resolvedQueue.add(resolvedSocket);
            }
        }
    }
}
