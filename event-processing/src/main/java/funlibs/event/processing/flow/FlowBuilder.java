package funlibs.event.processing.flow;

import funlibs.event.processing.processor.InvocationProcessor;
import funlibs.event.processing.processor.InvocationProcessor.InvocationProcessorBuilder;
import funlibs.event.processing.processor.LoggingProcessor;
import funlibs.event.processing.processor.LoggingProcessor.LoggingProcessorBuilder;
import funlibs.event.processing.processor.ResultCheckProcessor;
import funlibs.event.processing.processor.ResultCheckProcessor.ResultCheckProcessorBuilder;
import funlibs.event.processing.processor.RetryProcessor;
import funlibs.event.processing.processor.RetryProcessor.RetryProcessorBuilder;
import reactor.core.publisher.Flux;

public class FlowBuilder {
	private LoggingProcessorBuilder logging = new LoggingProcessorBuilder();
	private InvocationProcessorBuilder invocation = new InvocationProcessorBuilder();
	private ResultCheckProcessorBuilder resultCheck = new ResultCheckProcessorBuilder();
	private RetryProcessorBuilder retry = new RetryProcessorBuilder();

	public static FlowBuilder of() {
		return new FlowBuilder();
	}

	private FlowBuilder() {
	}

	public LoggingProcessorBuilder logging() {
		return logging;
	}

	public InvocationProcessorBuilder invocation() {
		return invocation;
	}

	public ResultCheckProcessorBuilder resultCheck() {
		return resultCheck;
	}

	public RetryProcessorBuilder retry() {
		return retry;
	}

	public Flow build() {
		LoggingProcessor loggingProcessor = logging.build();
		InvocationProcessor invocationProcessor = invocation.build();
		ResultCheckProcessor resultCheckProcessor = resultCheck.build();
		RetryProcessor retryProcessor = retry.build();
		return new Flow() //
			.loggingProcessor(loggingProcessor) //
			.invocationProcessor(invocationProcessor) //
			.resultCheckProcessor(resultCheckProcessor) //
			.retryProcessor(retryProcessor) //
			.onStart(() -> {
				Flux.from(retryProcessor).subscribe(invocationProcessor);
				Flux.from(resultCheckProcessor).filter(msg -> !msg.normal() && !msg.retry()).subscribe();
				Flux.from(invocationProcessor).filter(msg -> !msg.normal() && !msg.retry()).subscribe();
				Flux.from(invocationProcessor).filter(msg -> msg.normal()).subscribe();
				Flux.from(resultCheckProcessor).filter(msg -> !msg.normal() && msg.retry()).subscribe(retryProcessor);
				Flux.from(invocationProcessor).filter(msg -> !msg.normal() && msg.retry()).subscribe(retryProcessor);
				Flux.from(invocationProcessor).filter(msg -> msg.normal()).subscribe(resultCheckProcessor);
				Flux.from(loggingProcessor).subscribe(invocationProcessor);

				retryProcessor.start();
				resultCheckProcessor.start();
				invocationProcessor.start();
				loggingProcessor.start();
			}) //
			.onStop(() -> {
//				invocationProcessor.stop();
//				resultCheckProcessor.stop();
//				retryProcessor.stop();
			}) //
		;
	}
}
