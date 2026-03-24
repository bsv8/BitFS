import type { ReactNode } from "react";

type ModalProps = {
  title: string;
  open: boolean;
  onClose: () => void;
  children: ReactNode;
};

export function Modal({ title, open, onClose, children }: ModalProps) {
  if (!open) {
    return null;
  }
  return (
    <div className="modal-backdrop" onClick={(event) => event.target === event.currentTarget && onClose()}>
      <article className="modal-card">
        <div className="modal-head">
          <h3>{title}</h3>
          <button className="ghost-button" type="button" onClick={onClose}>关闭</button>
        </div>
        <div className="modal-body">{children}</div>
      </article>
    </div>
  );
}
