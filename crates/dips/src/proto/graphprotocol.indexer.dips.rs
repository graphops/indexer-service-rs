// This file is @generated by prost-build.
/// *
/// A request to propose a new _indexing agreement_ to an _indexer_.
///
/// See the `DipsService.SubmitAgreementProposal` method.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubmitAgreementProposalRequest {
    #[prost(uint64, tag = "1")]
    pub version: u64,
    /// / An ERC-712 signed indexing agreement voucher
    #[prost(bytes = "vec", tag = "2")]
    pub signed_voucher: ::prost::alloc::vec::Vec<u8>,
}
/// *
/// A response to a request to propose a new _indexing agreement_ to an _indexer_.
///
/// See the `DipsService.SubmitAgreementProposal` method.
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct SubmitAgreementProposalResponse {
    /// / The response to the agreement proposal.
    #[prost(enumeration = "ProposalResponse", tag = "1")]
    pub response: i32,
}
/// *
/// A request to cancel an _indexing agreement_.
///
/// See the `DipsService.CancelAgreement` method.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelAgreementRequest {
    #[prost(uint64, tag = "1")]
    pub version: u64,
    /// / a signed ERC-712 message cancelling an agreement
    #[prost(bytes = "vec", tag = "2")]
    pub signed_cancellation: ::prost::alloc::vec::Vec<u8>,
}
/// *
/// A response to a request to cancel an existing _indexing agreement_.
///
/// See the `DipsService.CancelAgreement` method.
///
/// Empty message, eventually we may add custom status codes
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct CancelAgreementResponse {}
/// *
/// The response to an _indexing agreement_ proposal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ProposalResponse {
    /// / The agreement proposal was accepted.
    Accept = 0,
    /// / The agreement proposal was rejected.
    Reject = 1,
}
impl ProposalResponse {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Accept => "ACCEPT",
            Self::Reject => "REJECT",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ACCEPT" => Some(Self::Accept),
            "REJECT" => Some(Self::Reject),
            _ => None,
        }
    }
}
/// Generated server implementations.
pub mod dips_service_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with DipsServiceServer.
    #[async_trait]
    pub trait DipsService: std::marker::Send + std::marker::Sync + 'static {
        /// *
        /// Propose a new _indexing agreement_ to an _indexer_.
        ///
        /// The _indexer_ can `ACCEPT` or `REJECT` the agreement.
        async fn submit_agreement_proposal(
            &self,
            request: tonic::Request<super::SubmitAgreementProposalRequest>,
        ) -> std::result::Result<
            tonic::Response<super::SubmitAgreementProposalResponse>,
            tonic::Status,
        >;
        /// *
        /// Request to cancel an existing _indexing agreement_.
        async fn cancel_agreement(
            &self,
            request: tonic::Request<super::CancelAgreementRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CancelAgreementResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct DipsServiceServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> DipsServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for DipsServiceServer<T>
    where
        T: DipsService,
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/graphprotocol.indexer.dips.DipsService/SubmitAgreementProposal" => {
                    #[allow(non_camel_case_types)]
                    struct SubmitAgreementProposalSvc<T: DipsService>(pub Arc<T>);
                    impl<
                        T: DipsService,
                    > tonic::server::UnaryService<super::SubmitAgreementProposalRequest>
                    for SubmitAgreementProposalSvc<T> {
                        type Response = super::SubmitAgreementProposalResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::SubmitAgreementProposalRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as DipsService>::submit_agreement_proposal(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = SubmitAgreementProposalSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/graphprotocol.indexer.dips.DipsService/CancelAgreement" => {
                    #[allow(non_camel_case_types)]
                    struct CancelAgreementSvc<T: DipsService>(pub Arc<T>);
                    impl<
                        T: DipsService,
                    > tonic::server::UnaryService<super::CancelAgreementRequest>
                    for CancelAgreementSvc<T> {
                        type Response = super::CancelAgreementResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CancelAgreementRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as DipsService>::cancel_agreement(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = CancelAgreementSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        let mut response = http::Response::new(empty_body());
                        let headers = response.headers_mut();
                        headers
                            .insert(
                                tonic::Status::GRPC_STATUS,
                                (tonic::Code::Unimplemented as i32).into(),
                            );
                        headers
                            .insert(
                                http::header::CONTENT_TYPE,
                                tonic::metadata::GRPC_CONTENT_TYPE,
                            );
                        Ok(response)
                    })
                }
            }
        }
    }
    impl<T> Clone for DipsServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    /// Generated gRPC service name
    pub const SERVICE_NAME: &str = "graphprotocol.indexer.dips.DipsService";
    impl<T> tonic::server::NamedService for DipsServiceServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
