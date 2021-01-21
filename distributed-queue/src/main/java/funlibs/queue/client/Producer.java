package funlibs.queue.client;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;

public class Producer {
	public Flux<ProduceResult> produce(Publisher<ProduceRequest> source) {
		return Flux.from(source).map(this::produce);
	}

	private ProduceResult produce(ProduceRequest request) {
		return null;
	}

	private CommitResult commit(CommitRequest request) {
		return null;
	}
}
