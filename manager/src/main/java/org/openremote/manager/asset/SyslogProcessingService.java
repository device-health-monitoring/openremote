/*
 * Copyright 2017, OpenRemote Inc.
 *
 * See the CONTRIBUTORS.txt file in the distribution for a
 * full listing of individual contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.openremote.manager.asset;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.openremote.container.message.MessageBrokerService;
import org.openremote.container.persistence.PersistenceService;
import org.openremote.container.timer.TimerService;
import org.openremote.model.Container;
import org.openremote.model.ContainerService;
import org.openremote.model.asset.agent.Protocol;
import org.openremote.model.attribute.Attribute;
import org.openremote.model.attribute.AttributeEvent;
import org.openremote.model.attribute.AttributeEvent.Source;
import org.openremote.model.syslog.SyslogEvent;
import org.openremote.model.syslog.SyslogReadFailure;
import org.openremote.model.util.ValueUtil;
import org.openremote.manager.event.ClientEventService;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.openremote.container.concurrent.GlobalLock.withLock;
import static org.openremote.model.attribute.AttributeEvent.HEADER_SOURCE;
import static org.openremote.model.attribute.AttributeEvent.Source.CLIENT;
import static org.openremote.model.attribute.AttributeEvent.Source.INTERNAL;
import static org.openremote.manager.event.ClientEventService.CLIENT_EVENT_TOPIC;

@SuppressWarnings("unchecked")
public class SyslogProcessingService extends RouteBuilder implements ContainerService {

    public static final int PRIORITY =1000;
    // TODO: Some of these options should be configurable depending on expected load etc.

    public static final String  SYSLOG_QUEUE = "seda://SyslogQueue?waitForTaskToComplete=IfReplyExpected&timeout=10000&purgeWhenStopping=true&discardIfNoConsumers=false&size=25000";
    private static final Logger LOG = Logger.getLogger(SyslogProcessingService.class.getName());
    protected TimerService timerService;
    protected PersistenceService persistenceService;

    protected MessageBrokerService messageBrokerService;
    protected ClientEventService clientEventService;
    // Used in testing to detect if initial/startup processing has completed
    protected long lastProcessedEventTimestamp = System.currentTimeMillis();

    protected static Processor handleSyslogProcessingException(Logger logger) {
        return exchange -> {
            SyslogEvent event = exchange.getIn().getBody(SyslogEvent.class);
            Exception exception = (Exception) exchange.getProperty(Exchange.EXCEPTION_CAUGHT);

            StringBuilder error = new StringBuilder();

            Source source = exchange.getIn().getHeader(HEADER_SOURCE, "unknown source", Source.class);
            if (source != null) {
                error.append("Error processing from ").append(source);
            }

            String protocolName = exchange.getIn().getHeader(Protocol.SENSOR_QUEUE_SOURCE_PROTOCOL, String.class);
            if (protocolName != null) {
                error.append(" (protocol: ").append(protocolName).append(")");
            }

            // TODO Better exception handling - dead letter queue?
            if (exception instanceof SyslogProcessingException) {
                SyslogProcessingException processingException = (SyslogProcessingException) exception;
                error.append(" - ").append(processingException.getMessage());
                error.append(": ").append(event.toString());
                logger.warning(error.toString());
            } else {
                error.append(": ").append(event.toString());
                logger.log(Level.WARNING, error.toString(), exception);
            }

            // Make the exception available if MEP is InOut
            exchange.getOut().setBody(exception);
        };
    }

    @Override
    public int getPriority() {
        return PRIORITY;
    }

    @Override
    public void init(Container container) throws Exception {
        timerService = container.getService(TimerService.class);
        persistenceService = container.getService(PersistenceService.class);

        messageBrokerService = container.getService(MessageBrokerService.class);
        clientEventService = container.getService(ClientEventService.class);

        container.getService(MessageBrokerService.class).getContext().addRoutes(this);
    }

    @Override
    public void start(Container container) throws Exception {

    }

    @Override
    public void stop(Container container) throws Exception {

    }

    @SuppressWarnings("rawtypes")
    @Override
    public void configure() throws Exception {

        from(CLIENT_EVENT_TOPIC)
                .routeId("FromClientUpdates")
                .filter(body().isInstanceOf(AttributeEvent.class))
                .setHeader(HEADER_SOURCE, () -> CLIENT)
                .to(SYSLOG_QUEUE);

        from(SYSLOG_QUEUE)
                .routeId("SyslogQueueProcessor")
                .filter(body().isInstanceOf(AttributeEvent.class))
                .doTry()
                .process(exchange -> withLock(getClass().getSimpleName() + "::processFromSyslogQueue", () -> {


                    AttributeEvent event = exchange.getIn().getBody(AttributeEvent.class);
                    LOG.finest("Processing: " + event);

                    Source source = exchange.getIn().getHeader(HEADER_SOURCE, () -> null, Source.class);
                    if (source == null) {
                        throw new SyslogProcessingException(SyslogReadFailure.MISSING_SOURCE);
                    }


                    Object value = event.getValue().orElse("");


                    processSyslog(value.toString());


                }))
                .endDoTry()
                .doCatch(SyslogProcessingException.class)
                .process(handleSyslogProcessingException(LOG));
    }

    /**
     * Send internal attribute change events into the {@link #SYSLOG_QUEUE}.
     */
    public void sendSyslogEvent(SyslogEvent syslogEvent) {
        sendSyslogEvent(syslogEvent, INTERNAL);
    }

    public void sendSyslogEvent(SyslogEvent syslogEvent, Source source) {
        // Set event source time if not already set
//        if (SyslogEvent.getTimestamp() <= 0) {
//            syslogEvent.setTimestamp(timerService.getCurrentTimeMillis());
//        }
        messageBrokerService.getProducerTemplate().sendBodyAndHeader(SYSLOG_QUEUE, syslogEvent, HEADER_SOURCE, source);
    }

    protected boolean processSyslog(String syslog) throws SyslogProcessingException {
        return true;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                '}';
    }
}
