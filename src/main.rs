use std::process::ExitCode;
use std::sync::Arc;

use tokio::sync::watch;

use badvpn_udpgw_rs::cli::{parse_args, print_help, print_version, PROGRAM_NAME, VERSION_STR};
use badvpn_udpgw_rs::{log_error, log_notice, logger, run_server};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let program = args
        .first()
        .map(String::as_str)
        .unwrap_or(PROGRAM_NAME);

    let cfg = match parse_args(&args) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("Failed to parse arguments: {err}");
            print_help(program);
            return ExitCode::FAILURE;
        }
    };

    if cfg.show_help {
        print_version();
        print_help(program);
        return ExitCode::SUCCESS;
    }

    if cfg.show_version {
        print_version();
        return ExitCode::SUCCESS;
    }

    logger::set_level(cfg.loglevel);
    log_notice!("initializing {} {}", PROGRAM_NAME, VERSION_STR);

    if cfg.listen_addrs.is_empty() {
        log_notice!("no --listen-addr specified; server will idle until terminated");
    }

    let num_threads = cfg.worker_threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });

    // Automatically select low-overhead single-threaded runtime on 1-vCPU VPS.
    let runtime = if num_threads <= 1 {
        log_notice!("runtime: current_thread (optimized for 1 vCPU)");
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
    } else {
        log_notice!("runtime: multi_thread ({} workers)", num_threads);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_threads)
            .enable_all()
            .build()
    };

    let runtime = match runtime {
        Ok(rt) => rt,
        Err(err) => {
            log_error!("failed to build tokio runtime: {}", err);
            return ExitCode::FAILURE;
        }
    };

    let cfg = Arc::new(cfg);

    let exit_result = runtime.block_on(async move {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        tokio::spawn(async move {
            wait_for_shutdown_signal().await;
            log_notice!("termination requested");
            let _ = shutdown_tx.send(true);
        });

        run_server(cfg, shutdown_rx).await
    });

    log_notice!("exiting");

    match exit_result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            log_error!("server exited with error: {}", err);
            ExitCode::FAILURE
        }
    }
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
