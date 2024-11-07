// Copyright 2023-, Edge & Node, GraphOps, and Semiotic Labs.
// SPDX-License-Identifier: Apache-2.0

//! This is a module that reimplements eventuals using
//! tokio::watch module and fixing some problems that eventuals
//! usually carry like initializing things without initializing
//! its values

use std::{future::Future, time::Duration};

use tokio::{
    select,
    sync::watch::{self, Ref},
    task::JoinHandle,
    time::{self, sleep},
};
use tracing::warn;

/// Creates a new watcher that auto initializes it with initial_value
/// and updates it given an interval
pub async fn new_watcher<T, F, Fut>(
    interval: Duration,
    function: F,
) -> anyhow::Result<watch::Receiver<T>>
where
    F: Fn() -> Fut + Send + 'static,
    T: Sync + Send + 'static,
    Fut: Future<Output = anyhow::Result<T>> + Send,
{
    let initial_value = function().await?;

    let (tx, rx) = watch::channel(initial_value);

    tokio::spawn(async move {
        let mut time_interval = time::interval(interval);
        time_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            time_interval.tick().await;
            let result = function().await;
            match result {
                Ok(value) => tx.send(value).expect("Failed to update channel"),
                Err(err) => {
                    // TODO mark it as delayed
                    warn!(error = %err, "There was an error while updating watcher");
                    // Sleep for a bit before we retry
                    sleep(interval.div_f32(2.0)).await;
                }
            }
        }
    });
    Ok(rx)
}

/// Join two watch::Receiver
pub fn join_and_map_watcher<T1, T2, T3, F>(
    mut receiver_1: watch::Receiver<T1>,
    mut receiver_2: watch::Receiver<T2>,
    map_function: F,
) -> watch::Receiver<T3>
where
    T1: Clone + Send + Sync + 'static,
    T2: Clone + Send + Sync + 'static,
    T3: Send + Sync + 'static,
    F: Fn((T1, T2)) -> T3 + Send + 'static,
{
    let initial_value = map_function((receiver_1.borrow().clone(), receiver_2.borrow().clone()));
    let (tx, rx) = watch::channel(initial_value);

    tokio::spawn(async move {
        loop {
            select! {
                Ok(())= receiver_1.changed() =>{},
                Ok(())= receiver_2.changed() =>{},
                else=>{
                    // Something is wrong.
                    panic!("receiver_1 or receiver_2 was dropped");
                }
            }

            let current_val_1 = receiver_1.borrow().clone();
            let current_val_2 = receiver_2.borrow().clone();
            let mapped_value = map_function((current_val_1, current_val_2));
            tx.send(mapped_value).expect("Failed to update channel");
        }
    });
    rx
}

// Replacement for pipe_async function in eventuals
// Listen to the changes in a receiver and runs parametric function
pub fn watch_pipe<T, F, Fut>(rx: watch::Receiver<T>, function: F) -> JoinHandle<()>
where
    T: Clone + Send + Sync + 'static,
    F: Fn(Ref<'_, T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let mut rx = rx;
        let value = rx.borrow();
        function(value).await;
        loop {
            let res = rx.changed().await;
            match res {
                Ok(_) => {
                    let value = rx.borrow();
                    function(value).await;
                }
                Err(err) => {
                    warn!("{err}");
                    break;
                }
            };
        }
    })
}
