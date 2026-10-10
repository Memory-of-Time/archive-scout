if __name__ == "__main__":
    import multiprocessing

    # Frozen workers must dispatch before the CLI parses their startup arguments.
    multiprocessing.freeze_support()

    from archive_scout.cli import main

    main()
