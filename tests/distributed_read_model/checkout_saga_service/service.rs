use std::sync::Arc;

use distributed::microsvc::{Routes, Service};

use super::{handlers, CheckoutRepo};

pub fn service(repo: CheckoutRepo) -> Arc<Service> {
    Arc::new(
        Service::new()
            .with_http_command_routes()
            .routes(distributed::routes!(
                Routes::new().with_repo(repo),
                command handlers::start,
                event handlers::record_seat_reserved,
            )),
    )
}

#[cfg(feature = "http")]
pub async fn start_http_service(service: Arc<Service>) -> String {
    let app = distributed::microsvc::router(service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("HTTP test server should bind");
    let addr = listener
        .local_addr()
        .expect("HTTP test server should expose local address");

    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("HTTP test server should serve");
    });

    format!("http://{addr}")
}

#[cfg(feature = "grpc")]
pub async fn start_grpc_service(
    service: Arc<Service>,
) -> distributed::microsvc::grpc::CommandServiceClient<tonic::transport::Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("gRPC test server should bind");
    let addr = listener
        .local_addr()
        .expect("gRPC test server should expose local address");
    let grpc_svc = distributed::microsvc::grpc_server(service);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(grpc_svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("gRPC test server should serve");
    });

    distributed::microsvc::grpc::CommandServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("gRPC test client should connect")
}
