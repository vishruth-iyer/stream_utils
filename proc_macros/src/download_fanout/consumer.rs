pub(crate) fn create_consumer_group_output_struct(
    mut item: syn::ItemStruct,
    ident: syn::Ident,
    fanout_consumer_group_output_derives: Option<syn::Attribute>,
) -> syn::ItemStruct {
    for field in &mut item.fields {
        field.attrs.clear();
        let ty = &field.ty;
        field.ty = syn::parse_quote! {
            <#ty as download_fanout::consumer::FanoutConsumerGroup>::Output
        };
    }
    item.ident = ident;
    item.attrs.clear();
    if let Some(fanout_consumer_group_output_derives) = fanout_consumer_group_output_derives {
        item.attrs.push(fanout_consumer_group_output_derives);
    }

    item
}

pub(crate) fn impl_consumer_group(
    consumer_group_struct: &syn::ItemStruct,
    output_struct_ident: &syn::Ident,
    error_ty: &syn::Type,
) -> syn::ItemImpl {
    let consumer_group_ident = &consumer_group_struct.ident;
    let (impl_generics, ty_generics, where_clause) =
        consumer_group_struct.generics.split_for_impl();

    let mut create_consumer_futures = Vec::with_capacity(consumer_group_struct.fields.len());
    let mut consumer_future_idents = Vec::with_capacity(consumer_group_struct.fields.len());
    let mut consumer_result_idents = Vec::with_capacity(consumer_group_struct.fields.len());
    let mut output_struct_init_lines = Vec::with_capacity(consumer_group_struct.fields.len());
    for member in consumer_group_struct.fields.members() {
        let (
            consumer_member,
            consumer_future_ident,
            consumer_result_ident,
            output_struct_init_line,
        ) = match member {
            syn::Member::Named(consumer_ident) => {
                let consumer_future_ident = quote::format_ident!("{consumer_ident}_future");
                let consumer_result_ident = quote::format_ident!("{consumer_ident}_result");
                let output_struct_init_line =
                    quote::quote! { #consumer_ident : #consumer_result_ident ? };
                (
                    syn::Member::Named(consumer_ident),
                    consumer_future_ident,
                    consumer_result_ident,
                    output_struct_init_line,
                )
            }
            syn::Member::Unnamed(consumer_index) => {
                let index = consumer_index.index;
                let consumer_future_ident = quote::format_ident!("future_{index}");
                let consumer_result_ident = quote::format_ident!("result_{index}");
                let output_struct_init_line = quote::quote! { #consumer_result_ident ? };
                (
                    syn::Member::Unnamed(consumer_index),
                    consumer_future_ident,
                    consumer_result_ident,
                    output_struct_init_line,
                )
            }
        };
        create_consumer_futures.push(quote::quote! {
            let (download_broadcaster, #consumer_future_ident) = self.#consumer_member._consume_from_fanout(download_broadcaster, content_length);
        });
        consumer_future_idents.push(consumer_future_ident);
        consumer_result_idents.push(consumer_result_ident);
        output_struct_init_lines.push(output_struct_init_line);
    }

    let output_struct_init = match consumer_group_struct.fields {
        syn::Fields::Named(_) => quote::quote! {
            #output_struct_ident {
                #(#output_struct_init_lines),*
            }
        },
        syn::Fields::Unnamed(_) => quote::quote! {
            #output_struct_ident (
                #(#output_struct_init_lines),*
            )
        },
        syn::Fields::Unit => quote::quote! {
            #output_struct_ident
        },
    };

    syn::parse_quote! {
        impl #impl_generics stream_utils::download_fanout::consumer::FanoutConsumerGroup for #consumer_group_ident #ty_generics #where_clause {
            type Output = #output_struct_ident;
            type Error = #error_ty;
            fn _consume_from_fanout<'a, 'b, Channel>(
                &'a self,
                download_broadcaster: &'b mut stream_utils::broadcaster::Broadcaster<bytes::Bytes, Channel>,
                content_length: Option<u64>,
            ) -> (
                &'b mut stream_utils::broadcaster::Broadcaster<bytes::Bytes, Channel>,
                impl Future<Output = Result<Self::Output, Self::Error>> + 'a,
            )
            where
                Channel: stream_utils::channel::Channel<bytes::Bytes>,
                Channel::Receiver: 'static
            {
                #(#create_consumer_futures)*

                let future = async {
                    let (
                        #(#consumer_result_idents),*,
                    ) = tokio::join!(
                        #(#consumer_future_idents),*
                    );
                    Ok(#output_struct_init)
                };
                (download_broadcaster, future)
            }
        }
    }
}
