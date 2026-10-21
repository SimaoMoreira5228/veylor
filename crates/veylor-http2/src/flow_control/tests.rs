use super::{
    DEFAULT_FLOW_WINDOW_SIZE, FlowWindow, FlowWindowError, MAX_FLOW_WINDOW_SIZE,
    choose_data_frame_size,
};

#[test]
fn initializes_windows_within_the_protocol_range() {
    assert_eq!(
        FlowWindow::new(DEFAULT_FLOW_WINDOW_SIZE),
        Ok(FlowWindow {
            value: DEFAULT_FLOW_WINDOW_SIZE,
        })
    );
    assert_eq!(FlowWindow::new(0).unwrap().value(), 0);
    assert_eq!(
        FlowWindow::new(MAX_FLOW_WINDOW_SIZE).unwrap().value(),
        MAX_FLOW_WINDOW_SIZE
    );
    assert_eq!(
        FlowWindow::new(-1),
        Err(FlowWindowError::InvalidInitialWindow)
    );
    assert_eq!(
        FlowWindow::new(MAX_FLOW_WINDOW_SIZE + 1),
        Err(FlowWindowError::InvalidInitialWindow)
    );
}

#[test]
fn consumes_available_window_without_overdraft() {
    let mut window = FlowWindow::new(100).unwrap();
    window.take(40).unwrap();
    assert_eq!(window.value(), 60);

    window.take(60).unwrap();
    assert_eq!(window.value(), 0);
    assert_eq!(window.take(1), Err(FlowWindowError::InsufficientWindow));
    assert_eq!(window.take(-1), Err(FlowWindowError::InvalidAmount));
    assert_eq!(window.value(), 0);
}

#[test]
fn rejects_window_increment_zero_and_overflow() {
    let mut window = FlowWindow::new(MAX_FLOW_WINDOW_SIZE - 10).unwrap();
    window.add(10).unwrap();
    assert_eq!(window.value(), MAX_FLOW_WINDOW_SIZE);
    assert_eq!(window.add(1), Err(FlowWindowError::Overflow));
    assert_eq!(window.value(), MAX_FLOW_WINDOW_SIZE);

    let mut empty = FlowWindow::new(0).unwrap();
    empty.add(65_535).unwrap();
    assert_eq!(empty.value(), 65_535);
    assert_eq!(empty.add(0), Err(FlowWindowError::InvalidIncrement));
}

#[test]
fn applies_initial_window_deltas_and_tracks_negative_credit() {
    let mut window = FlowWindow::new(65_535).unwrap();
    window.take(60_000).unwrap();
    window.adjust(-65_535).unwrap();
    assert_eq!(window.value(), -60_000);
    window.take(0).unwrap();
    assert_eq!(window.take(1), Err(FlowWindowError::InsufficientWindow));

    window.adjust(65_535).unwrap();
    assert_eq!(window.value(), 5_535);
    assert_eq!(
        window.adjust(i64::MAX),
        Err(FlowWindowError::InvalidAdjustment)
    );
    assert_eq!(window.value(), 5_535);

    let mut full = FlowWindow::new(MAX_FLOW_WINDOW_SIZE).unwrap();
    assert_eq!(full.adjust(1), Err(FlowWindowError::Overflow));
    assert_eq!(full.value(), MAX_FLOW_WINDOW_SIZE);
}

#[test]
fn chooses_data_frames_with_both_windows_and_frame_limit() {
    assert_eq!(
        choose_data_frame_size(65_535, 65_535, 65_535, 16_384),
        16_384
    );
    assert_eq!(choose_data_frame_size(100, 65_535, 1_000, 16_384), 100);
    assert_eq!(choose_data_frame_size(65_535, 50, 1_000, 16_384), 50);
    assert_eq!(choose_data_frame_size(65_535, 65_535, 100, 16_384), 100);
    assert_eq!(choose_data_frame_size(10, 10, 5, 16_384), 5);
    assert_eq!(choose_data_frame_size(0, 100, 100, 16_384), 0);
    assert_eq!(choose_data_frame_size(100, 0, 100, 16_384), 0);
    assert_eq!(choose_data_frame_size(-1, 100, 100, 16_384), 0);
    assert_eq!(choose_data_frame_size(100, 100, 0, 16_384), 0);
}
