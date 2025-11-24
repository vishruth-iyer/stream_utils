pub(crate) fn impl_consumer_group(
    consumer_group: &syn::DeriveInput,
    output_ident: &syn::Ident,
    item_ty: &syn::Type,
) -> syn::ItemImpl {
    let consumer_group_ident = &consumer_group.ident;
    let (impl_generics, ty_generics, where_clause) = consumer_group.generics.split_for_impl();

    match &consumer_group.data {
        syn::Data::Struct(consumer_group_struct) => {
            let mut create_consumer_futures =
                Vec::with_capacity(consumer_group_struct.fields.len());
            let mut consumer_future_idents = Vec::with_capacity(consumer_group_struct.fields.len());
            let mut consumer_result_idents = Vec::with_capacity(consumer_group_struct.fields.len());
            let mut output_struct_init_lines =
                Vec::with_capacity(consumer_group_struct.fields.len());
            let mut retry_struct_init_lines = Vec::with_capacity(consumer_group_struct.fields.len());
            for member in consumer_group_struct.fields.members() {
                let (
                    consumer_member,
                    consumer_future_ident,
                    consumer_result_ident,
                    output_struct_init_line,
                    retry_struct_init_line,
                ) = match member {
                    syn::Member::Named(consumer_ident) => {
                        let consumer_future_ident = quote::format_ident!("{consumer_ident}_future");
                        let consumer_result_ident = quote::format_ident!("{consumer_ident}_result");
                        let output_struct_init_line =
                            quote::quote! { #consumer_ident : #consumer_result_ident };
                        let retry_struct_init_line = quote::quote! { #consumer_ident : self.#consumer_ident.retry(&previous_output.#consumer_ident) };
                        (
                            syn::Member::Named(consumer_ident),
                            consumer_future_ident,
                            consumer_result_ident,
                            output_struct_init_line,
                            retry_struct_init_line,
                        )
                    }
                    syn::Member::Unnamed(consumer_index) => {
                        let index = consumer_index.index;
                        let consumer_future_ident = quote::format_ident!("future_{index}");
                        let consumer_result_ident = quote::format_ident!("result_{index}");
                        let output_struct_init_line = quote::quote! { #consumer_result_ident };
                        let retry_struct_init_line = quote::quote! { self.#consumer_index.retry(&previous_output.#consumer_index) };
                        (
                            syn::Member::Unnamed(consumer_index),
                            consumer_future_ident,
                            consumer_result_ident,
                            output_struct_init_line,
                            retry_struct_init_line,
                        )
                    }
                };
                create_consumer_futures.push(quote::quote! {
                    let (fanout_broadcaster, #consumer_future_ident) = self.#consumer_member._consume_from_fanout(fanout_broadcaster, content_length);
                });
                retry_struct_init_lines.push(retry_struct_init_line);
                consumer_future_idents.push(consumer_future_ident);
                consumer_result_idents.push(consumer_result_ident);
                output_struct_init_lines.push(output_struct_init_line);
            }

            let output_struct_init = match consumer_group_struct.fields {
                syn::Fields::Named(_) => quote::quote! {
                    #output_ident {
                        #(#output_struct_init_lines),*
                    }
                },
                syn::Fields::Unnamed(_) => quote::quote! {
                    #output_ident (
                        #(#output_struct_init_lines),*
                    )
                },
                syn::Fields::Unit => quote::quote! {
                    #output_ident
                },
            };

            let retry_struct_init = match consumer_group_struct.fields {
                syn::Fields::Named(_) => quote::quote! {
                    Self {
                        #(#retry_struct_init_lines),*
                    }
                },
                syn::Fields::Unnamed(_) => quote::quote! {
                    Self (
                        #(#retry_struct_init_lines),*
                    )
                },
                syn::Fields::Unit => quote::quote! {
                    Self
                }
            };

            syn::parse_quote! {
                #[automatically_derived]
                impl #impl_generics stream_utils::fanout::consumer::FanoutConsumerGroup for #consumer_group_ident #ty_generics #where_clause {
                    type Item = #item_ty;
                    type Output = #output_ident #ty_generics;
                    fn _consume_from_fanout<'fanout_consumer_group, 'fanout_consumer_group_broadcaster, Channel>(
                        &'fanout_consumer_group self,
                        fanout_broadcaster: &'fanout_consumer_group_broadcaster mut stream_utils::broadcaster::Broadcaster<Channel>,
                        content_length: Option<u64>,
                    ) -> (
                        &'fanout_consumer_group_broadcaster mut stream_utils::broadcaster::Broadcaster<Channel>,
                        impl std::future::Future<Output = Self::Output> + 'fanout_consumer_group,
                    )
                    where
                        Channel: stream_utils::channel::Channel<Item = bytes::Bytes>,
                        Channel::Receiver: 'static,
                    {
                        #(#create_consumer_futures)*

                        let future = async {
                            let (
                                #(#consumer_result_idents),*,
                            ) = tokio::join!(
                                #(#consumer_future_idents),*
                            );
                            #output_struct_init
                        };
                        (fanout_broadcaster, future)
                    }
                    fn retry(self, previous_output: &Self::Output) -> Self {
                        #retry_struct_init
                    }
                }
            }
        }
        syn::Data::Enum(_consumer_group_enum) => {
            todo!("Enums not currently supported");
        }
        syn::Data::Union(_consumer_group_union) => {
            todo!("Unions not currently supported");
        }
    }
}

pub(crate) fn create_consumer_group_output(
    consumer_group: syn::DeriveInput,
    output_ident: syn::Ident,
    fanout_consumer_group_output_derives: Option<syn::Attribute>,
) -> syn::DeriveInput {
    let mut consumer_group_output = consumer_group;
    match &mut consumer_group_output.data {
        syn::Data::Struct(consumer_group_output_struct) => {
            for field in &mut consumer_group_output_struct.fields {
                field.attrs.clear();
                let ty = &field.ty;
                field.ty = syn::parse_quote! {
                    <#ty as stream_utils::fanout::consumer::FanoutConsumerGroup>::Output
                };
            }
        }
        syn::Data::Enum(consumer_group_output_enum) => {
            for variant in &mut consumer_group_output_enum.variants {
                for field in &mut variant.fields {
                    field.attrs.clear();
                    let ty = &field.ty;
                    field.ty = syn::parse_quote! {
                        <#ty as stream_utils::fanout::consumer::FanoutConsumerGroup>::Output
                    };
                }
            }
        }
        syn::Data::Union(consumer_group_output_union) => {
            for field in &mut consumer_group_output_union.fields.named {
                field.attrs.clear();
                let ty = &field.ty;
                field.ty = syn::parse_quote! {
                    <#ty as stream_utils::fanout::consumer::FanoutConsumerGroup>::Output
                };
            }
        }
    }

    consumer_group_output.ident = output_ident;
    consumer_group_output.attrs.clear();
    if let Some(fanout_consumer_group_output_derives) = fanout_consumer_group_output_derives {
        consumer_group_output
            .attrs
            .push(fanout_consumer_group_output_derives);
    }

    consumer_group_output
}

pub(crate) fn impl_cancel_egress(
    consumer_group_output: &syn::DeriveInput,
) -> syn::ItemImpl {
    let consumer_group_output_ident = &consumer_group_output.ident;
    let (impl_generics, ty_generics, where_clause) = consumer_group_output.generics.split_for_impl();
    match &consumer_group_output.data {
        syn::Data::Struct(consumer_group_output_struct) => {
            let mut fields_cancel_egress = Vec::with_capacity(consumer_group_output_struct.fields.len());
            for member in consumer_group_output_struct.fields.members() {
                let field_cancel_egress = match member {
                    syn::Member::Named(ident) => quote::quote! {
                        self. #ident .cancel_egress()
                    },
                    syn::Member::Unnamed(index) => quote::quote! {
                        self. #index .cancel_egress()
                    },
                };
                fields_cancel_egress.push(field_cancel_egress);
            }
            syn::parse_quote! {
                #[automatically_derived]
                impl #impl_generics stream_utils::fanout::consumer::CancelEgress for #consumer_group_output_ident #ty_generics #where_clause {
                    fn cancel_egress(&self) -> bool {
                        #(#fields_cancel_egress) || *
                    }
                }
            }
        }
        syn::Data::Enum(_consumer_group_output_enum) => {
            todo!("Enums not currently supported");
        }
        syn::Data::Union(_consumer_group_output_union) => {
            todo!("Unions not currently supported");
        }
    }
}

pub(crate) fn impl_maybe_retryable(
    consumer_group_output: &syn::DeriveInput,
) -> syn::ItemImpl {
    let consumer_group_output_ident = &consumer_group_output.ident;
    let (impl_generics, ty_generics, where_clause) = consumer_group_output.generics.split_for_impl();
    match &consumer_group_output.data {
        syn::Data::Struct(consumer_group_output_struct) => {
            let mut fields_is_retryable = Vec::with_capacity(consumer_group_output_struct.fields.len());
            let mut fields_should_retry = Vec::with_capacity(consumer_group_output_struct.fields.len());
            for member in consumer_group_output_struct.fields.members() {
                let field_is_retryable = match &member {
                    syn::Member::Named(ident) => quote::quote! {
                        self. #ident .is_retryable()
                    },
                    syn::Member::Unnamed(index) => quote::quote! {
                        self. #index .is_retryable()
                    },
                };
                fields_is_retryable.push(field_is_retryable);
                let field_should_retry = match &member {
                    syn::Member::Named(ident) => quote::quote! {
                        self. #ident .should_retry()
                    },
                    syn::Member::Unnamed(index) => quote::quote! {
                        self. #index .should_retry()
                    },
                };
                fields_should_retry.push(field_should_retry);
            }
            syn::parse_quote! {
                #[automatically_derived]
                impl #impl_generics stream_utils::fanout::MaybeRetryable for #consumer_group_output_ident #ty_generics #where_clause {
                    fn is_retryable(&self) -> bool {
                        #(#fields_is_retryable) && *
                    }
                    fn should_retry(&self) -> bool {
                        #(#fields_should_retry) || *
                    }
                }
            }
        }
        syn::Data::Enum(_consumer_group_output_enum) => {
            todo!("Enums not currently supported");
        }
        syn::Data::Union(_consumer_group_output_union) => {
            todo!("Unions not currently supported");
        }
    }
}
